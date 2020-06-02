"""
Helper functions for flowbyactivity data
"""
import flowsa
from functools import reduce
import numpy as np
import pandas as pd
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, load_sector_crosswalk, sector_source_name, datapath


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

def create_fill_na_dict():
    fill_na_dict = {}
    for k,v in flow_by_activity_fields.items():
        if v[0]['dtype']=='str':
            fill_na_dict[k] = ""
        elif v[0]['dtype']=='int':
            fill_na_dict[k] = 9999
        elif v[0]['dtype']=='float':
            fill_na_dict[k] = 0.0
    return fill_na_dict

fba_fill_na_dict =  create_fill_na_dict()

def get_fba_groupby_cols():
    groupby_cols = []
    for k,v in flow_by_activity_fields.items():
        if v[0]['dtype']=='str':
            groupby_cols.append(k)
        elif v[0]['dtype']=='int':
            groupby_cols.append(k)
    #Do not use description for grouping
    groupby_cols.remove('Description')
    return groupby_cols

fba_default_grouping_fields = get_fba_groupby_cols()

def filter_by_geoscale(flowbyactivity_df, geoscale):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param flowbyactitvy_df:
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity
    """
    # filter by geoscale depends on Location System
    fips = []
    if flowbyactivity_df['LocationSystem'].str.contains('FIPS').any():
        # all_FIPS = read_stored_FIPS()
        if (geoscale == "national"):
            fips.append(US_FIPS)
        elif (geoscale == "state"):
            state_FIPS = get_state_FIPS()
            fips = list(state_FIPS['FIPS'])
        elif (geoscale == "county"):
            county_FIPS = get_county_FIPS()
            fips = list(county_FIPS['FIPS'])

    flowbyactivity_df = flowbyactivity_df[flowbyactivity_df['Location'].isin(fips)]

    if len(flowbyactivity_df) == 0:
        log.error("No flows found in the flow dataset at the " + geoscale + " scale.")
    else:
        return flowbyactivity_df

def agg_by_geoscale(flowbyactivity_df, from_scale, to_scale):
    """

    :param flowbyactivity_df:
    :param from_scale:
    :param to_scale:
    :return:
    """
    from flowsa.common import fips_number_key
    from_scale_dig = fips_number_key[from_scale]
    to_scale_dig = fips_number_key[to_scale]

    #use from scale to filter by these values
    fba_from_scale = filter_by_geoscale(flowbyactivity_df,from_scale)

    group_cols = fba_default_grouping_fields.copy()
    group_cols.remove('Location')

    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        fba_from_scale['to_Location'] = fba_from_scale['Location'].apply(lambda x: str(x[0:2]))
        group_cols.append('to_Location')
    #if national no need to do anything
    fba_agg = aggregator(fba_from_scale, group_cols)
    return fba_agg


def aggregator(flowbyactivity_df, groupbycols):
    """
    Aggregates flowbyactivity_df by given groupbycols
    :param flowbyactivity_df:
    :param groupbycols:
    :return:
    """

    try:
        wm = lambda x: np.ma.average(x, weights=flowbyactivity_df.loc[x.index, "FlowAmount"])
    except ZeroDivisionError:
        wm = 0

    agg_funx = {"FlowAmount":"sum",
                "Spread":wm,
                "DataReliability": wm,
                "DataCollection": wm}
    flowbyactivity_dfg = flowbyactivity_df.groupby(groupbycols, as_index=False).agg(agg_funx)
    return flowbyactivity_dfg


def add_missing_flow_by_activity_fields(flowbyactivity_partial_df):
    """
    Add in missing fields to have a complete and ordered
    :param flowbyactivity_partial_df:
    :return:
    """
    for k in flow_by_activity_fields.keys():
        if k not in flowbyactivity_partial_df.columns:
            flowbyactivity_partial_df[k] = None
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flow_by_activity_fields.items():
        flowbyactivity_partial_df[k] = flowbyactivity_partial_df[k].astype(v[0]['dtype'])
    # Resort it so order is correct
    flowbyactivity_partial_df = flowbyactivity_partial_df[flow_by_activity_fields.keys()]
    return flowbyactivity_partial_df

def check_fba_fields(flowbyactivity_df):
    """
    Add in missing fields to have a complete and ordered
    :param flowbyactivity_partial_df:
    :return:
    """
    for k,v in flow_by_activity_fields.items():
        try:
            log.debug("fba activity " + k + " data type is " + str(flowbyactivity_df[k].values.dtype))
            log.debug("standard " + k + " data type is " + str(v[0]['dtype']))
        except:
            log.debug("Failed to find field ",k," in fba")


def check_if_activities_match_sectors(fba):
    """
    Checks if activities in flowbyactivity that appear to be like sectors are actually sectors
    :param fba: a flow by activity dataset
    :return: A list of activities not marching the default sector list or text indicating 100% match
    """
    # Get list of activities in a flowbyactivity file
    activities = []
    for f in  fba_activity_fields:
        activities.extend(fba[f])
    activities.remove("None")

    # Get list of module default sectors
    flowsa_sector_list = list(load_sector_crosswalk()[sector_source_name])
    activities_missing_sectors = set(activities) - set(flowsa_sector_list)

    if (len(activities_missing_sectors) > 0):
        log.info(str(len(activities_missing_sectors)) + " activities not matching sectors in default " + sector_source_name + " list.")
        return activities_missing_sectors
    else:
        log.info("All activities match sectors in " + sector_source_name + " list.")
        return None


def convert_unit(df):
    """Convert unit to standard"""
    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2/yr
    # class = money, unit = USD/yr

    # class = water, unit = m3/yr
    df['FlowAmount'] = np.where(df['Unit'] == 'Bgal/d', ((df['FlowAmount'] * 1000000000) / 264.17) / 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Bgal/d', 'm3.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'Mgal/d', ((df['FlowAmount'] * 1000000) / 264.17) / 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Mgal/d', 'm3.yr', df['Unit'])

    # class = other, unit varies

    return df


def geoscale_flow_comparison(flowclass, years, datasource, activitynames='all', to_scale='national'):
    """ Aggregates county data to state and national, and state data to national level, allowing for comparisons
        in flow totals for a given flowclass and industry. First assigns all flownames to NAICS and standardizes units"""

    # problems with the code:
    # will not work for usgs wu because nat'l level data has different names. only works for datasets that have county
    # level data

    # load parquet file checking aggregation
    flows = flowsa.getFlowByActivity(flowclass=flowclass,
                                     years=years,
                                     datasource=datasource)
    # fill null values
    flows = flows.fillna(value=fba_fill_na_dict)
    # convert units
    flows = convert_unit(flows)

    # if activityname set to default, then compare aggregation for all activities. If looking at particular activity,
    # filter that activity out
    if activitynames == ['all']:
        flow_subset = flows.copy()
    else:
        flow_subset = flows[(flows[fba_activity_fields[0]].isin(activitynames)) |
                            (flows[fba_activity_fields[1]].isin(activitynames))]

    # Reset index values after subset
    flow_subset = flow_subset.reset_index()

    # create list of geoscales for aggregation
    if to_scale == 'national':
        geoscales = ['national', 'state', 'county']
    elif to_scale =='state':
        geoscales = ['state', 'county']

    # create empty df list
    flow_dfs = []
    for i in geoscales:
        # filter by geoscale
        fba_from_scale = filter_by_geoscale(flow_subset, i)

        # remove "location" as a column
        group_cols = fba_default_grouping_fields.copy()
        group_cols.remove('Location')

        # county sums to state and national, state sums to national
        if to_scale == 'state':
            fba_from_scale['to_Location'] = fba_from_scale['Location'].apply(lambda x: str(x[0:2]))
        elif to_scale == 'national':
            fba_from_scale['to_Location'] = US_FIPS
        group_cols.append('to_Location')

        # aggregate
        fba_agg = aggregator(fba_from_scale, group_cols)

        # rename flowamount column, based on geoscale
        fba_agg = fba_agg.rename(columns={"FlowAmount": "FlowAmount_" + i})

        # drop fields irrelevant to aggregated flow comparision
        drop_fields = flows[['MeasureofSpread', 'Spread', 'DistributionType', 'DataReliability','DataCollection']]
        fba_agg = fba_agg.drop(columns=drop_fields)

        # reset index
        fba_agg = fba_agg.reset_index(drop=True)

        #append to datatframe
        flow_dfs.append(fba_agg)

    # merge list of dfs by column
    flow_comparison = reduce(lambda left, right: pd.merge(left, right, on=['Class', 'SourceName', 'FlowName', 'Unit',
                                                                           'ActivityProducedBy','ActivityConsumedBy',
                                                                           'Compartment', 'to_Location',
                                                                           'LocationSystem', 'Year']), flow_dfs)

    return flow_comparison

