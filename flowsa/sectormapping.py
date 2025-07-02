# sectormapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import os.path
import pandas as pd
import numpy as np
from pathlib import Path
from esupy.mapping import apply_flow_mapping
import flowsa
import flowsa.flowbyactivity
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
        external_mappingpath = Path(fbsconfigpath).parent / "activitytosectormapping"
        if external_mappingpath.exists():
            activity_mapping_source_name = get_flowsa_base_name(
                external_mappingpath, mapfn, 'csv')
            if (external_mappingpath /
                f"{activity_mapping_source_name}.csv").is_file():
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


def assign_technological_correlation(mapping):
    """
    Assign technological correlation sources based on the difference between source and target sectors using
    https://github.com/USEPA/esupy/blob/main/DataQualityPedigreeMatrix.md
    as a guideline
    """

    # todo: modify tech assignments for cases where there is one:one parent:child relationships because a NAICS5 is
    #  the same as a NAICS6 in these situations, so the tech score should be the same for each

    tech_dict = {'-5': '1',
                 '-4': '1',
                 '-3': '1',
                 '-2': '1',
                 '-1': '1',
                 '0': '1',
                 '1': '2',
                 '2': '3',
                 '3': '4',
                 '4': '5',
                 '5': '5',
                 '6': '5',
                 '7': '5',
                 '8': '5'
                 }

    # load the sector length crosswalk
    naics_crosswalk = load_crosswalk("Sector_Levels")

    # assign sector lengths to compare to assign tech correlation
    # merge dfs
    for i in ['source', 'target']:
        mapping = (mapping
                  .merge(naics_crosswalk[['Sector', 'SectorLength']],
                         how='left',
                         left_on=[f'{i}_naics'],
                         right_on=['Sector'])
                  .drop(columns=['Sector'])
                  .rename(columns={'SectorLength': f'{i}Length'})
                  )
    mapping = mapping.assign(SectorDifference = mapping[
        'targetLength'].astype(int) - mapping['sourceLength'].astype(int))

    # determine difference in sector lengths between source and target and assign tech score
    mapping = mapping.assign(TechnologicalCorrelation=mapping["SectorDifference"].astype(str).apply(lambda x: tech_dict.get(x)))
    mapping["TechnologicalCorrelation"] = mapping["TechnologicalCorrelation"].map(int)

    # address special circumstances for BEA household/gov codes by dropping duplicates, keeping first assignment
    mapping = mapping.drop_duplicates(subset=['source_naics', 'target_naics'], keep="first")

    return mapping.drop(columns=['sourceLength', 'targetLength', 'SectorDifference'])


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
        log.info(f"Mapping flows in {from_fba_source} to material flow list")
        flow_type = 'WASTE_FLOW'
        ignore_source_name = True
    else:
        log.info(f"Mapping flows in {from_fba_source} to federal elementary flow list")
        if 'fedefl_mapping' in v:
            mapping_files = v['fedefl_mapping']
            ignore_source_name = True
        else:
            mapping_files = from_fba_source
        flow_type = 'ELEMENTARY_FLOW'

    fbs_mapped = map_flows(fbs, mapping_files, flow_type,
                           ignore_source_name, **kwargs)

    return fbs_mapped, mapping_files


def map_to_BEA_sectors(fbs_load, region, io_level, output_year,
                       bea_year=2012):
    """
    Map FBS sectors from NAICS to BEA, allocating by gross industry output.

    :param fbs_load: df completed FlowBySector collapsed to single 'Sector'
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param output_year: year for industry output
    :param bea_year: 2012 or 2017

    """

    bea = get_BEA_industry_output(region, io_level, output_year, bea_year)

    if io_level == 'summary':
        mapping_col = f'BEA_{bea_year}_Summary_Code'
    elif io_level == 'detail':
        mapping_col = f'BEA_{bea_year}_Detail_Code'

    # determine naics year in df
    naics_year = fbs_load['SectorSourceName'][0].split(
        "_", 1)[1].split("_", 1)[0]

    # Prepare NAICS:BEA mapping file
    mapping = (load_crosswalk(f'NAICS_to_BEA_Crosswalk_{bea_year}')
               .rename(columns={mapping_col: 'BEA',
                                f'NAICS_{naics_year}_Code': 'Sector'}))
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
    ## For FBS with both SPB and SCB, map sequentially
    if set(['SectorProducedBy', 'SectorConsumedBy']).issubset(fbs_load.columns):
        fbs = fbs_load.copy()
        for col in ['SectorProducedBy', 'SectorConsumedBy']:
            fbs = (fbs.merge(
                mapping.drop_duplicates(subset='Sector', keep=False),
                how='left',
                left_on=col,
                right_on='Sector'))
            fbs = fbs.merge(dup.drop(columns='Output'),
                            how='left',
                            left_on=[col, 'Location'],
                            right_on=['Sector', 'Location'],
                            suffixes=(None, '_y'))
            fbs['Allocation'] = fbs['Allocation'].fillna(1)
            fbs['BEA'] = fbs['BEA'].fillna(fbs['BEA_y'])
            fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['Allocation']
            fbs = (fbs
                   .drop(columns=[col, 'BEA_y', 'Allocation', 'Sector', 'Sector_y'])
                   .rename(columns={'BEA': col})
                   )
        fbs = fbs.dropna(subset=['SectorProducedBy', 'SectorConsumedBy'])

    else:
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
        fbs = fbs.assign(Sector=fbs['BEA'])

    fbs = (fbs.drop(columns=dq_fields +
                    ['SectorSourceName', 'BEA',
                     'BEA_y', 'Allocation'], errors='ignore'))

    if (abs(1-(sum(fbs['FlowAmount']) /
               sum(fbs_load['FlowAmount'])))) > 0.005:
        log.warning('Data loss upon BEA mapping')

    return fbs


def get_BEA_industry_output(region, io_level, output_year, bea_year=2012):
    """
    Get FlowByActivity for industry output from state or national datasets
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param output_year: year for industry output
    :param bea_year: 2012 or 2017
    """
    if region == 'state':
        fba = 'stateio_Industry_GO'
        if io_level == 'detail':
            raise TypeError ('detail models not available for states')
    elif region == 'national':
        fba = 'BEA_Detail_GrossOutput_IO'

    # Get output by BEA sector
    bea = flowsa.flowbyactivity.getFlowByActivity(fba, output_year)
    bea = (
        bea.drop(columns=bea.columns.difference(
            ['FlowAmount','ActivityProducedBy','Location']))
        .rename(columns={'FlowAmount':'Output',
                         'ActivityProducedBy': 'BEA'}))

    # If needed, aggregate from detail to summary
    if region == 'national' and io_level == 'summary':
        bea_mapping = (load_crosswalk(f'NAICS_to_BEA_Crosswalk_{bea_year}')
                       [[f'BEA_{bea_year}_Detail_Code',
                         f'BEA_{bea_year}_Summary_Code']]
                       .drop_duplicates()
                       .rename(columns={f'BEA_{bea_year}_Detail_Code': 'BEA'}))
        bea = (bea.merge(bea_mapping, how='left', on='BEA')
               .drop(columns=['BEA'])
               .rename(columns={f'BEA_{bea_year}_Summary_Code': 'BEA'}))
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

    log.info(f'Mapping flows in {source} to {material_crosswalk}')
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

if __name__ == "__main__":
    df = flowsa.sectormapping.map_to_BEA_sectors(
        flowsa.getFlowBySector('GHG_national_2019_m1')
              .rename(columns={'SectorProducedBy':'Sector'}),
        region='national', io_level='summary', output_year='2019')
