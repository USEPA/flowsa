"""
Helper functions for flowbyactivity and flowbysector data
"""

import flowsa
import pandas as pd
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, flow_by_sector_fields, flow_by_sector_collapsed_fields, load_sector_crosswalk, \
    sector_source_name, get_flow_by_groupby_cols, create_fill_na_dict, generalize_activity_field_names, \
    load_sector_length_crosswalk

fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)


def clean_df(df, flowbyfields, fill_na_dict):
    """

    :param df:
    :param flowbyfields: flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: fba_fill_na_dict or fbs_fill_na_dict
    :return:
    """

    # temporarily replace 'None' with None until old code modified
    df = df.replace({'None': None})
    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # drop description field, if exists
    if 'Description' in df.columns:
        df = df.drop(columns='Description')
    # fill null values
    df = df.fillna(value=fill_na_dict)
    # harmonize units across dfs
    df = harmonize_units(df)

    return df


def create_geoscale_list(df, geoscale, year='2015'):
    """
    Create a list of FIPS associated with given geoscale

    :param df: FlowBySector of FlowByActivity df
    :param geoscale: 'national', 'state', or 'county'
    :return: list of relevant FIPS
    """

    # filter by geoscale depends on Location System
    fips = []
    if df['LocationSystem'].str.contains('FIPS').any():
        # all_FIPS = read_stored_FIPS()
        if geoscale == "national":
            fips.append(US_FIPS)
        elif geoscale == "state":
            state_FIPS = get_state_FIPS(year)
            fips = list(state_FIPS['FIPS'])
        elif geoscale == "county":
            county_FIPS = get_county_FIPS(year)
            fips = list(county_FIPS['FIPS'])

    return fips


def filter_by_geoscale(df, geoscale, activitynames):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param df: Either flowbyactivity or flowbysector
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity or flowbysector
    """

    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.error("No flows found in the " + ', '.join(
            activitynames) + " flow dataset at the " + geoscale + " scale.")
    else:
        return df


def agg_by_geoscale(df, from_scale, to_scale, groupbycolumns, activitynames):
    """

    :param df: flowbyactivity or flowbysector df
    :param from_scale:
    :param to_scale:
    :param groupbycolumns: flowbyactivity or flowbysector default groupby columns
    :return:
    """

    # use from scale to filter by these values
    df = filter_by_geoscale(df, from_scale, activitynames).reset_index(drop=True)

    group_cols = groupbycolumns.copy()

    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        df.loc[:, 'Location'] = df['Location'].apply(lambda x: str(x[0:2]))
        # pad zeros
        df.loc[:, 'Location'] = df['Location'].apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5 else x)
    elif to_scale == 'national':
        df.loc[:, 'Location'] = US_FIPS

    fba_agg = aggregator(df, group_cols)

    return fba_agg


def aggregator(df, groupbycols):
    """
    Aggregates flowbyactivity or flowbysector df by given groupbycols

    :param df: Either flowbyactivity or flowbysector
    :param groupbycols: Either flowbyactivity or flowbysector columns
    :return:
    """

    # weighted average function
    try:
        wm = lambda x: np.ma.average(x, weights=df.loc[x.index, "FlowAmount"])
    except ZeroDivisionError:
        wm = 0

    # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
    possible_column_headers = ('Spread', 'Min', 'Max', 'DataReliability', 'TemporalCorrelation',
                               'GeographicalCorrelation', 'TechnologicalCorrelation',
                               'DataCollection')

    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in df.columns.values.tolist()]

    # initial dictionary of how a column should be aggregated
    agg_funx = {"FlowAmount": "sum"}

    # add columns to the aggregation dictionary that should be aggregated using a weighted avg
    for e in column_headers:
        agg_funx.update({e: wm})

    # aggregate df by groupby columns, either summing or creating weighted averages
    df_dfg = df.groupby(groupbycols, as_index=False).agg(agg_funx)

    return df_dfg


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields, flow_by_sector_fields, or flow_by_sector_collapsed_fields
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
    for k, v in flowbyfields.items():
        try:
            log.debug("fba activity " + k + " data type is " + str(flowby_df[k].values.dtype))
            log.debug("standard " + k + " data type is " + str(v[0]['dtype']))
        except:
            log.debug("Failed to find field ", k, " in fba")


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
    #activities.remove("None")

    # Get list of module default sectors
    flowsa_sector_list = list(load_sector_crosswalk()[sector_source_name])
    activities_missing_sectors = set(activities) - set(flowsa_sector_list)

    if len(activities_missing_sectors) > 0:
        log.info(str(len(
            activities_missing_sectors)) + " activities not matching sectors in default " + sector_source_name + " list.")
        return activities_missing_sectors
    else:
        log.info("All activities match sectors in " + sector_source_name + " list.")
        return None


def check_if_data_exists_at_geoscale(df, geoscale, activitynames='All'):
    """
    Check if an activity or a sector exists at the specified geoscale
    :param df: flowbyactivity dataframe
    :param activitynames: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :return:
    """

    # if any activity name is specified, check if activity data exists at the specified geoscale
    activity_list = []
    if activitynames != 'All':
        if isinstance(activitynames, str) == True:
            activity_list.append(activitynames)
        else:
            activity_list = activitynames
        # check for specified activity name
        df = df[(df[fba_activity_fields[0]].isin(activity_list)) |
                (df[fba_activity_fields[1]].isin(activity_list))].reset_index(drop=True)
    else:
        activity_list.append('activities')

    # filter by geoscale depends on Location System
    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.info(
            "No flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale.")
        exists = "No"
    else:
        log.info("Flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale.")
        exists = "Yes"

    return exists


def check_if_data_exists_at_less_aggregated_geoscale(df, geoscale, activityname):
    """
    In the event data does not exist at specified geoscale, check if data exists at less aggregated level

    :param df: Either flowbyactivity or flowbysector dataframe
    :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
    :return:
    """

    if geoscale == 'national':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
             df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'state')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + activityname + "  at the state scale.")
            fips = create_geoscale_list(df, 'county')
            df = df[df['Location'].isin(fips)]
            if len(df) == 0:
                log.info("No flows found for " + activityname + "  at the county scale.")
            else:
                log.info("Flowbyactivity data exists for " + activityname + " at the county level")
                new_geoscale_to_use = 'county'
                return new_geoscale_to_use
        else:
            log.info("Flowbyactivity data exists for " + activityname + " at the state level")
            new_geoscale_to_use = 'state'
            return new_geoscale_to_use
    if geoscale == 'state':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
             df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'county')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + activityname + "  at the county scale.")
        else:
            log.info("Flowbyactivity data exists for " + activityname + " at the county level")
            new_geoscale_to_use = 'county'
            return new_geoscale_to_use


def check_if_location_systems_match(df1, df2):
    """
    Check if two dataframes share the same location system
    :param df1: fba or fbs df
    :param df2: fba or fbs df
    :return:
    """

    if df1["LocationSystem"].all() == df2["LocationSystem"].all():
        log.info("LocationSystems match")
    else:
        log.warning("LocationSystems do not match, might lose county level data")


def check_if_data_exists_for_same_geoscales(fba_wsec_walloc, source,
                                            activity):  # fba_w_aggregated_sectors
    """
    Determine if data exists at the same scales for datasource and allocation source
    :param source_fba:
    :param allocation_fba:
    :return:
    """
    # todo: modify so only returns warning if no value for entire location, not just no value for one of the possible sectors

    from flowsa.mapping import get_activitytosector_mapping

    # create list of highest sector level for which there should be data
    mapping = get_activitytosector_mapping(source)
    # filter by activity of interest
    mapping = mapping.loc[mapping['Activity'].isin(activity)]
    # add sectors to list
    sectors_list = pd.unique(mapping['Sector']).tolist()

    # subset fba w sectors and with merged allocation table so only have rows with aggregated sector list
    df_subset = fba_wsec_walloc.loc[
        (fba_wsec_walloc[fbs_activity_fields[0]].isin(sectors_list)) |
        (fba_wsec_walloc[fbs_activity_fields[1]].isin(sectors_list))].reset_index(drop=True)
    # only interested in total flows
    # df_subset = df_subset.loc[df_subset['FlowName'] == 'total'].reset_index(drop=True)
    # df_subset = df_subset.loc[df_subset['Compartment'] == 'total'].reset_index(drop=True)

    # create subset of fba where the allocation data is missing
    missing_alloc = df_subset.loc[df_subset['FlowAmountRatio'].isna()].reset_index(drop=True)
    # drop any rows where source flow value = 0
    missing_alloc = missing_alloc.loc[missing_alloc['FlowAmount'] != 0].reset_index(drop=True)
    # create list of locations with missing alllocation data
    states_missing_data = pd.unique(missing_alloc['Location']).tolist()

    if len(missing_alloc) == 0:
        log.info("All aggregated sector flows have allocation flow ratio data")
    else:
        log.warning("Missing allocation flow ratio data for " + ', '.join(states_missing_data))

    return None


def harmonize_units(df):
    """
    Convert unit to standard
    :param df: Either flowbyactivity or flowbysector
    :return: Df with standarized units
    """
    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2/yr
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRES', df['FlowAmount'] * 4046.8564224,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRES', 'm2.yr', df['Unit'])

    # class = money, unit = USD/yr

    # class = water, unit = m3/yr
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day',
                                       (df['FlowAmount'] / 264.172052) * 365,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'm3.p.yr', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRE FEET / ACRE',
                                       (df['FlowAmount'] / 4046.856422) * 1233.481837,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', 'm3.m2.yr', df['Unit'])

    # class = other, unit varies

    return df


def allocate_by_sector(df_w_sectors, allocation_method):
    """
    Create an allocation ratio, after generalizing df so only one sector column

    :param df_w_sectors: df with single column of sectors
    :param allocation_method: currently written for 'proportional'
    :return: df with FlowAmountRatio for each sector
    """

    # group by columns, remove "FlowName" because some of the allocation tables have multiple variables and grouping
    # by them returns incorrect allocation ratios
    group_cols = fba_default_grouping_fields
    group_cols = [e for e in group_cols if
                  e not in ('ActivityProducedBy', 'ActivityConsumedBy', 'FlowName')]
    group_cols.append('Sector')

    # run sector aggregation fxn to determine total flowamount for each level of sector
    df = sector_aggregation_generalized(df_w_sectors, group_cols)
    # run sector disaggregation to capture one-to-one naics4/5/6 relationships
    df = sector_disaggregation_generalized(df)

    # if statements for method of allocation
    if allocation_method == 'proportional':
        # denominator summed from highest level of sector grouped by location
        denom_df = df.loc[df['Sector'].apply(lambda x: len(x) == 2)]
        denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount'].groupby(
            denom_df['Location']).transform('sum')
        denom_df = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
        # merge the denominator column with fba_w_sector df
        allocation_df = df.merge(denom_df, how='left')
        # calculate ratio
        allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
            'Denominator']
        allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

        return allocation_df


def allocation_helper(df_w_sector, method, attr):
    """
    Used when two df required to create allocation ratio
    :param df_w_sector:
    :param method: currently written for 'multiplication'
    :param attr:
    :return:
    """

    from flowsa.mapping import add_sectors_to_flowbyactivity

    helper_allocation = flowsa.getFlowByActivity(flowclass=[attr['helper_source_class']],
                                                 datasource=attr['helper_source'],
                                                 years=[attr['helper_source_year']])
    # clean df
    helper_allocation = clean_df(helper_allocation, flow_by_activity_fields, fba_fill_na_dict)
    # drop rows with flowamount = 0
    helper_allocation = helper_allocation[helper_allocation['FlowAmount'] != 0]

    # assign naics to allocation dataset
    helper_allocation = add_sectors_to_flowbyactivity(helper_allocation,
                                                      sectorsourcename=method[
                                                          'target_sector_source'],
                                                      levelofSectoragg=attr[
                                                          'helper_sector_aggregation'])
    # generalize activity field names to enable link to water withdrawal table
    helper_allocation = generalize_activity_field_names(helper_allocation)
    # drop columns
    helper_allocation = helper_allocation.drop(columns=['Activity', 'Min', 'Max'])
    # rename column
    helper_allocation = helper_allocation.rename(columns={"FlowAmount": 'HelperFlow'})

    # merge allocation df with helper df based on sectors, depending on geo scales of dfs
    if attr['helper_from_scale'] == 'national':
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Sector', 'HelperFlow']],
                                                    how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'state'):
        modified_fba_allocation = df_w_sector.merge(
            helper_allocation[['Sector', 'Location', 'HelperFlow']], how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'county'):
        helper_allocation.loc[:, 'Location_tmp'] = helper_allocation['Location'].apply(
            lambda x: str(x[0:2]))
        df_w_sector.loc[:, 'Location_tmp'] = df_w_sector['Location'].apply(lambda x: str(x[0:2]))
        modified_fba_allocation = df_w_sector.merge(
            helper_allocation[['Sector', 'Location_tmp', 'HelperFlow']],
            how='left')
        modified_fba_allocation = modified_fba_allocation.drop(columns=['Location_tmp'])

    # todo: modify so if missing data, replaced with value from one geoscale up instead of national
    # if missing values (na or 0), replace with national level values
    replacement_values = helper_allocation[helper_allocation['Location'] == US_FIPS].reset_index(
        drop=True)
    replacement_values = replacement_values.rename(columns={"HelperFlow": 'ReplacementValue'})
    modified_fba_allocation = modified_fba_allocation.merge(
        replacement_values[['Sector', 'ReplacementValue']], how='left')
    modified_fba_allocation.loc[:, 'HelperFlow'] = modified_fba_allocation['HelperFlow'].fillna(
        modified_fba_allocation['ReplacementValue'])
    modified_fba_allocation.loc[:, 'HelperFlow'] = np.where(modified_fba_allocation['HelperFlow'] == 0,
                                                            modified_fba_allocation['ReplacementValue'],
                                                            modified_fba_allocation['HelperFlow'])
    # modify flow amounts using helper data
    if attr['helper_method'] == 'multiplication':
        # replace non-existent helper flow values with a 0, so after multiplying, don't have incorrect value associated
        # with new unit
        modified_fba_allocation['HelperFlow'] = modified_fba_allocation['HelperFlow'].fillna(
            value=0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = modified_fba_allocation['FlowAmount'] * \
                                                       modified_fba_allocation[
                                                           'HelperFlow']
    # drop columns
    modified_fba_allocation = modified_fba_allocation.drop(
        columns=["HelperFlow", 'ReplacementValue'])

    # drop rows of 0 to speed up allocation
    modified_fba_allocation = modified_fba_allocation[
        modified_fba_allocation['FlowAmount'] != 0].reset_index(drop=True)

    #todo: modify the unit

    return modified_fba_allocation


def sector_aggregation_generalized(df, group_cols):
    """
    If a sector value is not included in df, sum together less aggregated sectors to calculate value.
    This function works for df with one sector column called "Sector"
    :param df: A df with a 'Sector' column
    :param group_cols: columns to group aggregation by
    :return: A df with sector levels summed from the least aggregated level
    """

    # drop any columns that contain a "-" in sector column
    df = df[~df['Sector'].str.contains('-', regex=True)].reset_index(drop=True)

    # find the longest length sector
    length = max(df['Sector'].apply(lambda x: len(x)).unique())
    # for loop in reverse order longest length naics minus 1 to 2 digit
    # appends missing naics levels to df
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df['Sector'].apply(lambda x: i + 2 > len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_subset = df_subset[['Sector']].drop_duplicates().reset_index(drop=True)
        sector_list = sector_subset['Sector'].apply(
            lambda x: x[0:i]).drop_duplicates().values.tolist()
        # create a list of sectors that are exactly i digits long
        existing_sectors = sector_subset.loc[sector_subset['Sector'].apply(lambda x: len(x) == i)]
        existing_sectors = existing_sectors['Sector'].drop_duplicates().dropna().values.tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = [e for e in sector_list if e not in existing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            agg_sectors_list = []
            for x in missing_sectors:
                # subset data
                agg_sectors_list.append(df_subset.loc[df_subset['Sector'].str.startswith(x)])
            agg_sectors = pd.concat(agg_sectors_list, sort=False)
            agg_sectors = agg_sectors.loc[agg_sectors['Sector'].apply(lambda x: len(x) > i)]
            agg_sectors.loc[:, 'Sector'] = agg_sectors['Sector'].apply(lambda x: str(x[0:i]))
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            df = df.append(agg_sectors, sort=False).reset_index(drop=True)

    return df


def sector_aggregation(df, group_cols):
    """
    Function that checks if a sector length exists, and if not, sums the less aggregated sector
    :param df: Either a flowbyactivity df with sectors or a flowbysector df
    :param group_cols: columns by which to aggregate
    :return:
    """

    #drop any columns that contain a "-" in sector column
    df = df[(~df[fbs_activity_fields[0]].str.contains('-', regex=True)) |
            (~df[fbs_activity_fields[1]].str.contains('-', regex=True))].reset_index(drop=True)

    # find the longest length sector
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).max().max()
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                           df[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_subset = df_subset[
            ['Location', fbs_activity_fields[0], fbs_activity_fields[1]]].drop_duplicates().reset_index(
            drop=True)
        df_sectors = sector_subset.copy()
        df_sectors.loc[:, 'SectorProducedBy'] = df_sectors['SectorProducedBy'].apply(lambda x: x[0:i])
        df_sectors.loc[:, 'SectorConsumedBy'] = df_sectors['SectorConsumedBy'].apply(lambda x: x[0:i])
        sector_list = df_sectors.drop_duplicates().values.tolist()
        # create a list of sectors that are exactly i digits long
        # where either sector column is i digits in length
        df_existing_1 = sector_subset.loc[(sector_subset['SectorProducedBy'].apply(lambda x: len(x) == i)) |
                                          (sector_subset['SectorConsumedBy'].apply(lambda x: len(x) == i))]
        # where both sector colums are i digits in length
        df_existing_2 = sector_subset.loc[(sector_subset['SectorProducedBy'].apply(lambda x: len(x) == i)) &
                                          (sector_subset['SectorConsumedBy'].apply(lambda x: len(x) == i))]
        # concat existing dfs
        df_existing = pd.concat([df_existing_1, df_existing_2], sort=False)
        existing_sectors = df_existing.drop_duplicates().dropna().values.tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = [e for e in sector_list if e not in existing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            agg_sectors_list = []
            for x, y, z in missing_sectors:
                c1 = df_subset['Location'] == x
                c2 = df_subset[fbs_activity_fields[0]].str.startswith(y)
                c3 = df_subset[fbs_activity_fields[1]].str.startswith(z)
                # subset data
                agg_sectors_list.append(df_subset.loc[c1 & c2 & c3])
            agg_sectors = pd.concat(agg_sectors_list, sort=False)
            agg_sectors = agg_sectors.loc[
                (agg_sectors[fbs_activity_fields[0]].apply(lambda x: len(x) > i)) |
                (agg_sectors[fbs_activity_fields[1]].apply(lambda x: len(x) > i))]
            agg_sectors.loc[:, fbs_activity_fields[0]] = agg_sectors[fbs_activity_fields[0]].apply(
                lambda x: str(x[0:i]))
            agg_sectors.loc[:, fbs_activity_fields[1]] = agg_sectors[fbs_activity_fields[1]].apply(
                lambda x: str(x[0:i]))
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            df = df.append(agg_sectors).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df.loc[:, 'SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorConsumedBy'])  # domestic/household
    df.loc[:, 'SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorProducedBy'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()

    return df


def sector_disaggregation(sector_disaggregation):
    """
    function to disaggregate sectors if there is only one naics at a lower level
    works for lower than naics 4
    :param df: A FBS df
    :return: A FBS df with missing naics5 and naics6
    """

    #todo: need to modify so works with either a fBA with sectors or a FBS because called on in a fxn \
    # that accepts either

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk()
    cw = cw_load[['NAICS_4', 'NAICS_5', 'NAICS_6']]

    # subset the naics 4 and 5 columsn
    cw4 = cw_load[['NAICS_4', 'NAICS_5']]
    cw4 = cw4.drop_duplicates(subset=['NAICS_4'], keep=False).reset_index(drop=True)
    naics4 = cw4['NAICS_4'].values.tolist()

    # subset the naics 5 and 6 columsn
    cw5 = cw_load[['NAICS_5', 'NAICS_6']]
    cw5 = cw5.drop_duplicates(subset=['NAICS_5'], keep=False).reset_index(drop=True)
    naics5 = cw5['NAICS_5'].values.tolist()

    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(4, 6):
        sector_disaggregation = clean_df(sector_disaggregation, flow_by_sector_fields, fbs_fill_na_dict)

        if i == 4:
            sector_list = naics4
            sector_merge = "NAICS_4"
            sector_add = "NAICS_5"
        elif i == 5:
            sector_list = naics5
            sector_merge = "NAICS_5"
            sector_add = "NAICS_6"

        # subset df to sectors with length = i and length = i + 1
        df_subset = sector_disaggregation.loc[sector_disaggregation[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                                              sector_disaggregation[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset.loc[:, 'SectorProduced_tmp'] = df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i])
        df_subset.loc[:, 'SectorConsumed_tmp'] = df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i])
        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset_1 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) |
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        df_subset_2 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) &
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        # concat existing dfs
        df_subset = pd.concat([df_subset_1, df_subset_2], sort=False)
        # drop all rows with duplicate temp values, as a less aggregated naics exists
        df_subset = df_subset.drop_duplicates(subset=['Flowable', 'Context', 'Location', 'SectorProduced_tmp',
                                                      'SectorConsumed_tmp'], keep=False).reset_index(drop=True)
        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorProduced_tmp'], right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SPB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        new_naics = pd.merge(new_naics, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorConsumed_tmp'], right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SCB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        # drop columns and rename new sector columns
        new_naics = new_naics.drop(columns=["SectorProducedBy", "SectorConsumedBy", "SectorProduced_tmp",
                                            "SectorConsumed_tmp"])
        new_naics = new_naics.rename(columns={"SPB": "SectorProducedBy",
                                              "SCB": "SectorConsumedBy"})
        # append new naics to df
        sector_disaggregation = pd.concat([sector_disaggregation, new_naics], sort=True)

    return sector_disaggregation


def sector_disaggregation_generalized(fbs):
    """
    function to disaggregate sectors if there is only one naics at a lower level
    works for lower than naics 4
    :param df: A FBS df
    :return: A FBS df with missing naics5 and naics6
    """

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk()
    cw = cw_load[['NAICS_4', 'NAICS_5', 'NAICS_6']]

    # subset the naics 4 and 5 columsn
    cw4 = cw_load[['NAICS_4', 'NAICS_5']]
    cw4 = cw4.drop_duplicates(subset=['NAICS_4'], keep=False).reset_index(drop=True)
    naics4 = cw4['NAICS_4'].values.tolist()

    # subset the naics 5 and 6 columsn
    cw5 = cw_load[['NAICS_5', 'NAICS_6']]
    cw5 = cw5.drop_duplicates(subset=['NAICS_5'], keep=False).reset_index(drop=True)
    naics5 = cw5['NAICS_5'].values.tolist()

    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(4, 6):

        if i == 4:
            sector_list = naics4
            sector_merge = "NAICS_4"
            sector_add = "NAICS_5"
        elif i == 5:
            sector_list = naics5
            sector_merge = "NAICS_5"
            sector_add = "NAICS_6"

        # subset df to sectors with length = i and length = i + 1
        df_subset = fbs.loc[fbs['Sector'].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset.loc[:, 'Sector_tmp'] = df_subset['Sector'].apply(lambda x: x[0:i])
        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset = df_subset.loc[df_subset['Sector_tmp'].isin(sector_list)]
        # drop all rows with duplicate temp values, as a less aggregated naics exists
        df_subset = df_subset.drop_duplicates(subset=['FlowName', 'Compartment', 'Location', 'Sector_tmp'],
                                              keep=False).reset_index(drop=True)
        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['Sector_tmp'], right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "ST"})
        new_naics = new_naics.drop(columns=[sector_merge])
        # drop columns and rename new sector columns
        new_naics = new_naics.drop(columns=["Sector", "Sector_tmp"])
        new_naics = new_naics.rename(columns={"ST": "Sector"})
        # append new naics to df
        fbs = pd.concat([fbs, new_naics], sort=True)

    return fbs


def assign_fips_location_system(df, year_of_data):
    """
    Add location system based on year of data. County level FIPS change over the years.
    :param df: df with FIPS location system
    :param year_of_data: year of data pulled
    :return:
    """

    if '2015' <= year_of_data:
        df.loc[:, 'LocationSystem'] = 'FIPS_2015'
    elif '2013' <= year_of_data < '2015':
        df.loc[:, 'LocationSystem'] = 'FIPS_2013'
    elif '2010' <= year_of_data < '2013':
        df.loc[:, 'LocationSystem'] = 'FIPS_2010'
    elif year_of_data < '2010':
        log.warning(
            "Missing FIPS codes from crosswalk for " + year_of_data + ". Temporarily assigning to FIPS_2010")
        df.loc[:, 'LocationSystem'] = 'FIPS_2010'
    return df


def collapse_fbs_sectors(fbs):
    """
    Collapses the Sector Produced/Consumed into a single column named "Sector"
    uses
    :param fbs: a standard FlowBySector (format)
    :return:
    """

    # collapse the FBS sector columns into one column based on FlowType
    fbs.loc[:, 'Sector'] = np.where(fbs["FlowType"] == 'TECHNOSPHERE_FLOW', fbs["SectorConsumedBy"], "None")
    fbs.loc[:, 'Sector'] = np.where(fbs["FlowType"] == 'WASTE_FLOW', fbs["SectorProducedBy"],
                                    fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where(
        (fbs["FlowType"] == 'WASTE_FLOW') & (fbs['SectorProducedBy'] == 'None'),
        fbs["SectorConsumedBy"], fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where(
        (fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorProducedBy'] == 'None'),
        fbs["SectorConsumedBy"], fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where(
        (fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'] == 'None'),
        fbs["SectorProducedBy"], fbs['Sector'])

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # reorder df columns and ensure correct datatype
    fbs_collapsed = add_missing_flow_by_fields(fbs_collapsed, flow_by_sector_collapsed_fields)
    # sort dataframe
    fbs_collapsed = fbs_collapsed.sort_values(
        ['Location', 'Flowable', 'Context', 'Sector']).reset_index(drop=True)

    return fbs_collapsed