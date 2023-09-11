# sectormapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import os.path
import pandas as pd
import numpy as np
from esupy.mapping import apply_flow_mapping
import flowsa
from flowsa.common import get_flowsa_base_name, load_crosswalk
from flowsa.dataclean import standardize_units
from flowsa.flowsa_log import log
from flowsa.schema import dq_fields


def get_activitytosector_mapping(source, fbsconfigpath=None):
    """
    Gets  the activity-to-sector mapping
    :param source: str, the data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    from flowsa.settings import crosswalkpath
    # identify mapping file name
    mapfn = f'NAICS_Crosswalk_{source}'

    # if FBS method file loaded from outside the flowsa directory, check if
    # there is also a crosswalk
    if fbsconfigpath is not None:
        external_mappingpath = (f"{os.path.dirname(fbsconfigpath)}"
                                "/activitytosectormapping/")
        if os.path.exists(external_mappingpath):
            activity_mapping_source_name = get_flowsa_base_name(
                external_mappingpath, mapfn, 'csv')
            if os.path.isfile(f"{external_mappingpath}"
                              f"{activity_mapping_source_name}.csv"):
                log.info(f"Loading {activity_mapping_source_name}.csv "
                         f"from {external_mappingpath}")
                crosswalkpath = external_mappingpath
    activity_mapping_source_name = get_flowsa_base_name(
        crosswalkpath, mapfn, 'csv')
    mapping = pd.read_csv(crosswalkpath / f'{activity_mapping_source_name}.csv',
                          dtype={'Activity': 'str', 'Sector': 'str'})
    # some mapping tables will have data for multiple sources, while other
    # mapping tables are used for multiple sources (like EPA_NEI or BEA
    # mentioned above) so if find the exact source name in the
    # ActivitySourceName column use those rows if the mapping file returns
    # empty, use the original mapping file subset df to keep rows where
    # ActivitySourceName matches source name
    mapping2 = mapping[mapping['ActivitySourceName'] == source].reset_index(
        drop=True)
    if len(mapping2) > 0:
        return mapping2
    else:
        return mapping


def convert_units_to_annual(df):
    """
    Convert data and units to annual flows
    :param df: df with 'FlowAmount' and 'Unit' column
    :return: df with annual FlowAmounts
    """
    # convert unit per day to year
    df['FlowAmount'] = np.where(df['Unit'].str.contains('/d'),
                                df['FlowAmount'] * 365,
                                df['FlowAmount'])
    df['Unit'] = df['Unit'].apply(lambda x: x.replace('/d', ""))

    return df


def map_flows(fba, from_fba_source, flow_type='ELEMENTARY_FLOW',
              ignore_source_name=False, **kwargs):
    """
    Applies mapping via esupy from fedelemflowlist or material
    flow list to convert flows to standardized list of flows
    :param fba: df flow-by-activity or flow-by-sector
    :param from_fba_source: str Source name of fba list to look for mappings
    :param flow_type: str either 'ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW',
        or 'WASTE_FLOW'
    :param ignore_source_name: bool, passed to apply_flow_mapping
    :param kwargs: optional - keep_unmapped_rows: False if want
        unmapped rows dropped, True if want to retain and keep_fba_columns:
        boolean, True or False, indicate if want to maintain
        'FlowName' and 'Compartment' columns in returned df
    :return: df, with flows mapped using federal elementary flow list or
        material flow list
    """

    # prior to mapping elementary flows, ensure all data
    # are in an annual format
    fba = convert_units_to_annual(fba)

    keep_unmapped_rows = False

    # if need to maintain FBA columns, create copies of columns
    if kwargs != {}:
        if ('keep_fba_columns' in kwargs) & \
                (kwargs['keep_fba_columns'] is True):
            fba['Flowable'] = fba['FlowName']
            fba['Context'] = fba['Compartment']
        # if keep unmapped rows identified in kwargs, then use
        if 'keep_unmapped_rows' in kwargs:
            keep_unmapped_rows = kwargs['keep_unmapped_rows']

    # else, rename
    else:
        fba = fba.rename(columns={'FlowName': 'Flowable',
                                  'Compartment': 'Context'})

    mapped_df = apply_flow_mapping(fba, from_fba_source,
                                   flow_type=flow_type,
                                   keep_unmapped_rows=keep_unmapped_rows,
                                   ignore_source_name=ignore_source_name)

    if mapped_df is None or len(mapped_df) == 0:
        # return the original df but with columns renamed so
        # can continue working on the FBS
        log.warning("Error in flow mapping, flows not mapped, returning FBA "
                    "with standardized units, but no standardized "
                    "Flowable, Context, or FlowUUID")
        mapped_df = fba.copy()
        mapped_df['FlowUUID'] = None
        mapped_df = standardize_units(mapped_df)

    return mapped_df


def map_fbs_flows(fbs, from_fba_source, v, **kwargs):
    """
    Identifies the mapping file and applies mapping to fbs flows
    :param fbs: flow-by-sector dataframe
    :param from_fba_source: str Source name of fba list to look for mappings
    :param v: dictionary, The datasource parameters
    :param kwargs: includes keep_unmapped_columns and keep_fba_columns
    :return fbs_mapped: df, with flows mapped using federal elementary
           flow list or material flow list
    :return mapping_files: str, name of mapping file
    """
    ignore_source_name = False
    if 'mfl_mapping' in v:
        mapping_files = v['mfl_mapping']
        log.info("Mapping flows in %s to material flow list", from_fba_source)
        flow_type = 'WASTE_FLOW'
        ignore_source_name = True
    else:
        log.info("Mapping flows in %s to federal elementary flow list",
                 from_fba_source)
        if 'fedefl_mapping' in v:
            mapping_files = v['fedefl_mapping']
            ignore_source_name = True
        else:
            mapping_files = from_fba_source
        flow_type = 'ELEMENTARY_FLOW'

    fbs_mapped = map_flows(fbs, mapping_files, flow_type,
                           ignore_source_name, **kwargs)

    return fbs_mapped, mapping_files


def map_to_BEA_sectors(fbs_load, region, io_level, year):
    """
    Map FBS sectors from NAICS to BEA, allocating by gross industry output.

    :param fbs_load: df completed FlowBySector collapsed to single 'Sector'
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param year: year for industry output
    """
    from flowsa.sectormapping import get_activitytosector_mapping

    bea = get_BEA_industry_output(region, io_level, year)

    if io_level == 'summary':
        mapping_col = 'BEA_2012_Summary_Code'
    elif io_level == 'detail':
        mapping_col = 'BEA_2012_Detail_Code'

    # Prepare NAICS:BEA mapping file
    mapping = (load_crosswalk('BEA')
               .rename(columns={mapping_col: 'BEA',
                                'NAICS_2012_Code': 'Sector'}))
    mapping = (mapping.drop(
        columns=mapping.columns.difference(['Sector','BEA']))
        .drop_duplicates(ignore_index=True)
        .dropna(subset=['Sector']))
    mapping['Sector'] = mapping['Sector'].astype(str)

    # Create allocation ratios where one to many NAICS:BEA
    dup = mapping[mapping['Sector'].duplicated(keep=False)]
    dup = dup.merge(bea, how='left', on='BEA')
    dup['Allocation'] = dup['Output']/dup.groupby(
        ['Sector','Location']).Output.transform('sum')

    # Update and allocate to sectors
    fbs = (fbs_load.merge(
        mapping.drop_duplicates(subset='Sector', keep=False),
        how='left',
        on='Sector'))
    fbs = fbs.merge(dup.drop(columns='Output'),
                    how='left', on=['Sector', 'Location'],
                    suffixes=(None, '_y'))
    fbs['Allocation'] = fbs['Allocation'].fillna(1)
    fbs['BEA'] = fbs['BEA'].fillna(fbs['BEA_y'])
    fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['Allocation']

    fbs = (fbs.drop(columns=dq_fields +
                    ['Sector', 'SectorSourceName',
                     'BEA_y', 'Allocation'], errors='ignore')
           .rename(columns={'BEA':'Sector'}))

    if (abs(1-(sum(fbs['FlowAmount']) /
               sum(fbs_load['FlowAmount'])))) > 0.005:
        log.warning('Data loss upon BEA mapping')

    return fbs


def get_BEA_industry_output(region, io_level, year):
    """
    Get FlowByActivity for industry output from state or national datasets
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param year: year for industry output
    """
    if region == 'state':
        fba = 'stateio_Industry_GO'
        if io_level == 'detail':
            raise TypeError ('detail models not available for states')
    elif region == 'national':
        fba = 'BEA_Detail_GrossOutput_IO'

    # Get output by BEA sector
    bea = flowsa.getFlowByActivity(fba, year)
    bea = (
        bea.drop(columns=bea.columns.difference(
            ['FlowAmount','ActivityProducedBy','Location']))
        .rename(columns={'FlowAmount':'Output',
                         'ActivityProducedBy': 'BEA'}))

    # If needed, aggregate from detial to summary
    if region == 'national' and io_level == 'summary':
        bea_mapping = (load_crosswalk('BEA')
                       [['BEA_2012_Detail_Code','BEA_2012_Summary_Code']]
                       .drop_duplicates()
                       .rename(columns={'BEA_2012_Detail_Code': 'BEA'}))
        bea = (bea.merge(bea_mapping, how='left', on='BEA')
               .drop(columns=['BEA'])
               .rename(columns={'BEA_2012_Summary_Code': 'BEA'}))
        bea = (bea.groupby(['BEA','Location']).agg({'Output': 'sum'})
               .reset_index())

    return bea


def map_to_material_crosswalk(df, source, source_attr):
    """
    Map df to a material crosswalk specified in the FBS method yaml.
    Material crosswalk will standardize material names
    :param df: df to be standardized
    :param source: str, name of FBA to standardize
    :param source_attr: dict, FBA
    :return: df with standardized material names
    """

    # determine if should map flows using file defined in fbs method
    material_crosswalk = source_attr.get('material_crosswalk')
    field_names = source_attr.get('material_crosswalk_field_dict')

    log.info(f'Mapping flows in %s to %s', source, material_crosswalk)
    mapped_df = apply_flow_mapping(df, source,
                                   flow_type='ELEMENTARY_FLOW',
                                   field_dict=field_names,
                                   material_crosswalk=material_crosswalk)

    mapped_df = mapped_df.replace('n.a.', np.nan)

    if mapped_df is None or len(mapped_df) == 0:
        # return the original df but with columns renamed so
        # can continue working on the FBS
        log.warning("Error in mapping, flows not mapped to material "
                    "crosswalk")
        mapped_df = df.copy()

    return mapped_df


def append_material_code(df, v, attr):
    """
    Append the sector commodity code to sectors using file specified in FBS
    method yaml
    :param df:
    :return:
    """
    mapping_file = pd.read_csv(v['append_material_codes'])

    # if material is identified in the activity set, use that material to
    # append the abbreviation, if not, then merge the mapping file to the df
    if attr.get('material') is not None:
        mapping_dict = mapping_file.set_index('Material').to_dict()['Abbr']
        abbr = mapping_dict.get(attr.get('material'))
        for s in ['SectorProducedBy', 'SectorConsumedBy']:
            df[s] = np.where((df[s] is not None) and (df[s] != ''),
                             df[s] + abbr, df[s])
    else:
        # add materials
        df = df.merge(mapping_file, left_on='Flowable', right_on='Material')
        for s in ['SectorProducedBy', 'SectorConsumedBy']:
            df[s] = np.where((df[s] is not None) and (df[s] != ''),
                             df[s] + df['Abbr'], df[s])
        # drop cols from mapping file
        df = df.drop(columns=['Material', 'Abbr'])

    return df
