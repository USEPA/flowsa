"""
Helper functions for flowbyactivity and flowbysector data
"""

import flowsa
import numpy as np
import pandas as pd
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, flow_by_sector_fields, load_sector_crosswalk, sector_source_name, datapath,\
    get_flow_by_groupby_cols, create_fill_na_dict, generalize_activity_field_names
from flowsa.mapping import expand_naics_list, add_sectors_to_flowbyactivity


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)


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
    """
    Convert unit to standard
    :param df: Either flowbyactivity or flowbysector
    :return: Df with standarized units
    """
    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2/yr
    df['FlowAmount'] = np.where(df['Unit'] == 'ACRES', df['FlowAmount'] * 4046.8564224, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'ACRES', 'm2.yr', df['Unit'])

    # class = money, unit = USD/yr

    # class = water, unit = m3/yr
    df['FlowAmount'] = np.where(df['Unit'] == 'Bgal/d', ((df['FlowAmount'] * 1000000000) / 264.17) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Bgal/d', 'm3.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'Mgal/d', ((df['FlowAmount'] * 1000000) / 264.17) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'Mgal/d', 'm3.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day', (df['FlowAmount'] / 264.172052) * 365, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'm3.p.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', (df['FlowAmount'] / 4046.856422) * 1233.481837, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', 'm3.m2.yr', df['Unit'])

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


def allocate_by_sector(fba_w_sectors, allocation_method):
    """

    :param fba_w_sectors:
    :param allocation_method:
    :return:
    """
    # drop any columns that contain a "-" in sector column
    fba_w_sectors = fba_w_sectors[~fba_w_sectors['Sector'].str.contains('-', regex=True)]

    # group by columns, remove "FlowName" because some of the allocation tables have multiple variables and grouping
    # by them returns incorrect allocation ratios
    group_cols = fba_default_grouping_fields
    group_cols = [e for e in group_cols if e not in ('ActivityProducedBy', 'ActivityConsumedBy', 'FlowName')]
    group_cols.append('Sector')

    # run sector aggregation fxn to determine total flowamount for each level of sector
    fba_w_sectors = sector_aggregation(fba_w_sectors, group_cols)

    # if statements for method of allocation
    if allocation_method == 'proportional':
        # denominator summed from highest level of sector grouped by location
        denom_df = fba_w_sectors.loc[fba_w_sectors['Sector'].apply(lambda x: len(x) == 2)]
        denom_df['Denominator'] = denom_df['FlowAmount'].groupby(denom_df['Location']).transform('sum')
        denom_df = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
        # merge the denominator column with fba_w_sector df
        allocation_df = fba_w_sectors.merge(denom_df, how='left')
        # calculate ratio
        allocation_df['FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df['Denominator']
        allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

        return allocation_df


def allocation_helper(fba_w_sector, method, attr):
    """

    :param fba_w_sector:
    :param method:
    :param attr:
    :return:
    """
    helper_allocation = flowsa.getFlowByActivity(flowclass=[attr['helper_source_class']],
                                                 datasource=attr['helper_source'],
                                                 years=[attr['helper_source_year']])
    # fill null values
    helper_allocation = helper_allocation.fillna(value=fba_fill_na_dict)
    # convert unit
    helper_allocation = convert_unit(helper_allocation)

    # assign naics to allocation dataset
    helper_allocation = add_sectors_to_flowbyactivity(helper_allocation,
                                                      sectorsourcename=method['target_sector_source'],
                                                      levelofSectoragg=attr['helper_sector_aggregation'])
    # generalize activity field names to enable link to water withdrawal table
    helper_allocation = generalize_activity_field_names(helper_allocation)
    # drop columns
    helper_allocation = helper_allocation.drop(columns=['Activity', 'Description', 'Min', 'Max'])
    # rename column
    helper_allocation = helper_allocation.rename(columns={"FlowAmount": 'HelperFlow'})

    # merge allocation df with helper df based on sectors, depending on geo scales of dfs
    if attr['helper_from_scale'] == 'national':
        modified_fba_allocation = fba_w_sector.merge(helper_allocation[['Sector', 'HelperFlow']], how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'county'):
        helper_allocation['Location_tmp'] = helper_allocation['Location'].apply(lambda x: str(x[0:2]))
        fba_w_sector['Location_tmp'] = fba_w_sector['Location'].apply(lambda x: str(x[0:2]))
        modified_fba_allocation = fba_w_sector.merge(helper_allocation[['Sector', 'Location_tmp', 'HelperFlow']],
                                                     how='left')
        modified_fba_allocation = modified_fba_allocation.drop(columns=['Location_tmp'])

    # modify flow amounts using helper data
    if attr['helper_method'] == 'multiplication':
        modified_fba_allocation['FlowAmount'] = modified_fba_allocation['FlowAmount'] * modified_fba_allocation[
            'HelperFlow']
    # drop columns
    modified_fba_allocation = modified_fba_allocation.drop(columns="HelperFlow")

    return modified_fba_allocation


def sector_aggregation(fbs_df, group_cols):
    """
    Function that checks if a sector aggregation exists, and if not, sums the less aggregated sector
    :param fbs_df: flow by sector dataframe
    :return:
    """

    # find the longest length naics (will be 6 or 8), needs to be integer for for loop
    length = max(fbs_df['Sector'].apply(lambda x: len(x)).unique())
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = fbs_df.loc[fbs_df['Sector'].apply(lambda x: i + 2 > len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_list = df_subset['Sector'].apply(lambda x: str(x[0:i])).unique().tolist()
        # create a list of sectors that are exactly i digits long
        existing_sectors = df_subset['Sector'].loc[df_subset['Sector'].apply(lambda x: len(x) == i)].unique().tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = np.setdiff1d(sector_list, existing_sectors).tolist()
        # add start of symbol to missing list
        missing_sectors = ["^" + e for e in missing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop the last digit of the sector and sum flow amounts
            agg_sectors = df_subset.loc[df_subset['Sector'].str.contains('|'.join(missing_sectors))]
            # only keep data with length greater than i
            agg_sectors = agg_sectors.loc[agg_sectors['Sector'].apply(lambda x: len(x) > i)]
            agg_sectors['Sector'] = agg_sectors['Sector'].apply(lambda x: str(x[0:i]))
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            fbs_df = fbs_df.append(agg_sectors, sort=True)

    # sort df
    fbs_df = fbs_df.sort_values(['Location', 'Sector'])

    return fbs_df
