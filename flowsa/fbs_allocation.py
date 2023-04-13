# fbs_allocation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to allocate data using additional data sources
"""

import flowsa
from flowsa.common import fba_activity_fields
from flowsa.settings import log
from flowsa.flowbyfunctions import subset_df_by_geoscale, \
    load_fba_w_standardized_units
from flowsa.sectormapping import add_sectors_to_flowbyactivity
from flowsa.validation import check_if_data_exists_at_geoscale


def load_map_clean_fba(method, attr, fba_sourcename, df_year, flowclass,
                       geoscale_from, geoscale_to, fbsconfigpath=None,
                       **kwargs):
    """
    Load, clean, and map a FlowByActivity df
    :param method: dictionary, FBS method yaml
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fba_sourcename: str, source name
    :param df_year: str, year
    :param flowclass: str, flowclass to subset df with
    :param geoscale_from: str, geoscale to use
    :param geoscale_to: str, geoscale to aggregate to
    :param kwargs: dictionary, can include parameters: 'allocation_flow',
                   'allocation_compartment','clean_allocation_fba',
                   'clean_allocation_fba_w_sec'
    :return: df, fba format
    """
    from flowsa.sectormapping import get_activitytosector_mapping
    # dictionary to load/standardize fba
    kwargs_dict = {}
    if 'download_FBA_if_missing' in kwargs:
        kwargs_dict['download_FBA_if_missing'] = \
            kwargs['download_FBA_if_missing']
    if 'allocation_map_to_flow_list' in attr:
        kwargs_dict['allocation_map_to_flow_list'] = \
            attr['allocation_map_to_flow_list']
    if 'allocation_fba_load_scale' in attr:
        kwargs_dict['geographic_level'] = attr['allocation_fba_load_scale']

    log.info("Loading allocation flowbyactivity %s for year %s",
             fba_sourcename, str(df_year))
    fba = load_fba_w_standardized_units(datasource=fba_sourcename,
                                        year=df_year,
                                        flowclass=flowclass,
                                        **kwargs_dict
                                        )

    # subset based on yaml settings
    if 'flowname_subset' in kwargs:
        if kwargs['flowname_subset'] != 'None':
            fba = fba.loc[fba['FlowName'].isin(kwargs['flowname_subset'])]
    if 'compartment_subset' in kwargs:
        if kwargs['compartment_subset'] != 'None':
            fba = \
                fba.loc[fba['Compartment'].isin(kwargs['compartment_subset'])]
    if 'allocation_selection_fields' in kwargs:
        selection_fields = attr.get('allocation_selection_fields')
        for k, v in selection_fields.items():
            fba = fba[fba[k].isin(v)].reset_index(drop=True)
    fba = (fba
           .drop(columns='Description')
           .reset_index(drop=True)
           )

    if len(fba) == 0:
        raise flowsa.exceptions.FBSMethodConstructionError(
            message='Allocation dataset is length 0; check flow or '
            'compartment subset for errors')

    # load relevant activities if activities are not naics-like
    try:
        sm = get_activitytosector_mapping(
            fba_sourcename, fbsconfigpath=fbsconfigpath)
        sm_list = sm['Activity'].drop_duplicates().values.tolist()
        # subset fba data by activities listed in the sector crosswalk
        fba = fba[(fba[fba_activity_fields[0]].isin(sm_list)) |
                  (fba[fba_activity_fields[1]].isin(sm_list)
                   )].reset_index(drop=True)
    except FileNotFoundError:
        pass

    # check if allocation data exists at specified geoscale to use
    log.info("Checking if allocation data exists at the %s level",
             geoscale_from)
    check_if_data_exists_at_geoscale(fba, geoscale_from)

    # aggregate geographically to the scale of the flowbyactivity source,
    # if necessary
    fba2 = subset_df_by_geoscale(fba, geoscale_from, geoscale_to)

    # cleanup the fba allocation df, if necessary
    if 'clean_fba' in kwargs:
        log.info("Cleaning %s", fba_sourcename)
        fba2 = kwargs["clean_fba"](
            fba2,
            attr=attr,
            download_FBA_if_missing=kwargs['download_FBA_if_missing']
        )
    # reset index
    fba2 = fba2.reset_index(drop=True)
    try:
        if kwargs.get('clean_fba_w_sec').__name__ in ('subset_and_equally_allocate_BEA_table'):
            fba2 = fba2.assign(group_id=fba2.reset_index().index.astype(str))
    except AttributeError:
        pass

    if len(fba2) == 0:
        raise flowsa.exceptions.FBSMethodConstructionError(
            message='Allocation dataset is length 0 after cleaning')

    # assign sector to allocation dataset
    activity_to_sector_mapping = attr.get('activity_to_sector_mapping')
    if 'activity_to_sector_mapping' in kwargs:
        activity_to_sector_mapping = kwargs.get('activity_to_sector_mapping')
    log.info("Adding sectors to %s", fba_sourcename)

    fba_wsec = add_sectors_to_flowbyactivity(
        fba2,
        sectorsourcename=method['target_sector_source'],
        activity_to_sector_mapping=activity_to_sector_mapping,
        overwrite_sectorlevel=attr.get(
            'activity_to_sector_aggregation_level'),
        fbsconfigpath=fbsconfigpath
    )

    # call on fxn to further clean up/disaggregate the fba
    # allocation data, if exists
    if 'clean_fba_w_sec' in kwargs:
        log.info("Further disaggregating sectors in %s", fba_sourcename)
        fba_wsec = kwargs['clean_fba_w_sec'](
            fba_wsec,
            attr=attr,
            method=method,
            sourcename=fba_sourcename,
            download_FBA_if_missing=kwargs['download_FBA_if_missing']
        )

    # drop group_id, which are used in some clean_fba_w_sec fnxs
    fba_wsec = fba_wsec.drop(columns=['group_id'], errors='ignore')

    return fba_wsec
