"""
Helper functions for flowbyactivity data
"""
import flowsa
import numpy as np
import pandas as pd
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, load_sector_crosswalk, sector_source_name, datapath, create_fill_na_dict,\
    get_flow_by_groupby_cols
from flowsa.mapping import expand_naics_list


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)

def filter_by_geoscale(df, geoscale):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param df: Either flowbyactivity or flowbysector
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity
    """
    # filter by geoscale depends on Location System
    fips = []
    if df['LocationSystem'].str.contains('FIPS').any():
        # all_FIPS = read_stored_FIPS()
        if (geoscale == "national"):
            fips.append(US_FIPS)
        elif (geoscale == "state"):
            state_FIPS = get_state_FIPS()
            fips = list(state_FIPS['FIPS'])
        elif (geoscale == "county"):
            county_FIPS = get_county_FIPS()
            fips = list(county_FIPS['FIPS'])

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.error("No flows found in the flow dataset at the " + geoscale + " scale.")
    else:
        return df

def agg_by_geoscale(df, from_scale, to_scale, groupbycolumns):
    """

    :param df: flowbyactivity or flowbysector df
    :param from_scale:
    :param to_scale:
    :param groupbycolumns: flowbyactivity or flowbysector default groupby columns
    :return:
    """
    from flowsa.common import fips_number_key

    from_scale_dig = fips_number_key[from_scale]
    to_scale_dig = fips_number_key[to_scale]

    #use from scale to filter by these values
    df_from_scale = filter_by_geoscale(df, from_scale)

    group_cols = groupbycolumns.copy()
    group_cols.remove('Location')

    # code for when the "Location" is a FIPS based system
    if to_scale == 'county':
        # drop rows that contain '000'
        df_from_scale = df_from_scale[~df_from_scale['Location'].str.contains("000")]
        df_from_scale['to_Location'] = df_from_scale['Location']
        group_cols.append('to_Location')
    elif to_scale == 'state':
         df_from_scale['to_Location'] = df_from_scale['Location'].apply(lambda x: str(x[0:2]))
         group_cols.append('to_Location')
    elif to_scale == 'national':
        df_from_scale['to_Location'] = US_FIPS
        group_cols.append('to_Location')
    fba_agg = aggregator(df_from_scale, group_cols)
    return fba_agg


def aggregator(df, groupbycols):
    """
    Aggregates flowbyactivity or flowbysector df by given groupbycols

    :param df: Either flowbyactivity or flowbysector
    :param groupbycols: Either flowbyactivity or flowbysector columns
    :return:
    """

    try:
        wm = lambda x: np.ma.average(x, weights=df.loc[x.index, "FlowAmount"])
    except ZeroDivisionError:
        wm = 0

    agg_funx = {"FlowAmount":"sum",
                "Spread":wm,
                "DataReliability": wm,
                "DataCollection": wm}

    df_dfg = df.groupby(groupbycols, as_index=False).agg(agg_funx)

    return df_dfg


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields or flow_by_sector_fields
    :return:
    """
    for k in flowbyfields.keys():
        if k not in flowby_partial_df.columns:
            flowby_partial_df[k] = None
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flowbyfields.items():
        flowby_partial_df[k] = flowby_partial_df[k].astype(v[0]['dtype'])
    # Resort it so order is correct
    flowby_partial_df = flowby_partial_df[flowbyfields.keys()]
    return flowby_partial_df


def check_flow_by_fields(flowby_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered
    :param flowby_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields or flow_by_sector_fields
    :return:
    """
    for k,v in flowbyfields.items():
        try:
            log.debug("fba activity " + k + " data type is " + str(flowby_df[k].values.dtype))
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
    for f in fba_activity_fields:
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
    df['FlowAmount'] = np.where(df['Unit'] == 'Bgal/d', ((df['FlowAmount'] * 1000000000) / 264.17) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Bgal/d', 'm3.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'Mgal/d', ((df['FlowAmount'] * 1000000) / 264.17) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Mgal/d', 'm3.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day', (df['FlowAmount'] / 264.172052) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'm3.p.yr', df['Unit'])

    # class = other, unit varies

    return df


def get_fba_allocation_subset(fba_allocation, source, activitynames):
    """
    Subset the fba allocation data based on NAICS associated with activity
    :param fba_allocation:
    :param sourcename:
    :param activitynames:
    :return:
    """

    # read in source crosswalk
    df = pd.read_csv(datapath+'activitytosectormapping/'+'Crosswalk_'+source+'_toNAICS.csv')
    sector_source_name = df['SectorSourceName'].all()
    df = expand_naics_list(df, sector_source_name)
    # subset source crosswalk to only contain values pertaining to list of activity names
    df = df.loc[df['Activity'].isin(activitynames)]
    # turn column of sectors related to activity names into list
    sector_list = pd.unique(df['Sector']).tolist()
    # subset fba allocation table to the values in the activity list, based on overlapping sectors
    fba_allocation_subset = fba_allocation.loc[(fba_allocation[fbs_activity_fields[0]].isin(sector_list)) |
                                               (fba_allocation[fbs_activity_fields[1]].isin(sector_list))]

    return fba_allocation_subset

