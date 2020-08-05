"""
Helper functions for flowbyactivity and flowbysector data
"""

import flowsa
import pandas as pd
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, flow_by_sector_fields, load_sector_crosswalk, sector_source_name, \
    get_flow_by_groupby_cols, create_fill_na_dict, generalize_activity_field_names

fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)


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
        log.error("No flows found in the " + ', '.join(activitynames) + " flow dataset at the " + geoscale + " scale.")
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
    #testing
    # df = fbs.copy()
    # groupbycolumns = fbs_default_grouping_fields.copy()
    # activitynames = names.copy()

    from flowsa.common import fips_number_key

    from_scale_dig = fips_number_key[from_scale]
    to_scale_dig = fips_number_key[to_scale]

    # use from scale to filter by these values
    df_from_scale = filter_by_geoscale(df, from_scale, activitynames)

    group_cols = groupbycolumns.copy()

    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        df_from_scale['Location'] = df_from_scale['Location'].apply(lambda x: str(x[0:2]))
    elif to_scale == 'national':
        df_from_scale['Location'] = US_FIPS

    fba_agg = aggregator(df_from_scale, group_cols)

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
                               'GeographicCorrelation', 'TechnologicalCorrelation', 'DataCollection')

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
    activities.remove("None")

    # Get list of module default sectors
    flowsa_sector_list = list(load_sector_crosswalk()[sector_source_name])
    activities_missing_sectors = set(activities) - set(flowsa_sector_list)

    if (len(activities_missing_sectors) > 0):
        log.info(str(len(
            activities_missing_sectors)) + " activities not matching sectors in default " + sector_source_name + " list.")
        return activities_missing_sectors
    else:
        log.info("All activities match sectors in " + sector_source_name + " list.")
        return None


def check_if_data_exists_at_geoscale(df, activitynames, geoscale):
    """
    Check if an activity or a sector exists at the specified geoscale
    :param df: Either flowbyactivity or flowbysector dataframe
    :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
    :return:
    """
    # filter by geoscale depends on Location System
    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.info("No flows found for " + ', '.join(activitynames) + " at the " + geoscale + " scale.")
        exists = "No"
    else:
        log.info("Flows found for " + ', '.join(activitynames) + " at the " + geoscale + " scale.")
        exists = "Yes"
    return exists


def check_if_data_exists_at_less_aggregated_geoscale(df, activitynames, geoscale):
    """
    In the event data does not exist at specified geoscale, check if data exists at less aggregated level

    :param df: Either flowbyactivity or flowbysector dataframe
    :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
    :return:
    """

    if geoscale == 'national':
        fips = create_geoscale_list(df, 'state')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + ', '.join(activitynames) + "  at the state scale.")
            fips = create_geoscale_list(df, 'county')
            df = df[df['Location'].isin(fips)]
            if len(df) == 0:
                log.info("No flows found for " + ', '.join(activitynames) + "  at the county scale.")
            else:
                log.info("Flowbyactivity data exists for " + ', '.join(activitynames) + " at the county level")
                new_geoscale_to_use = 'county'
                return new_geoscale_to_use
        else:
            log.info("Flowbyactivity data exists for " + ', '.join(activitynames) + " at the state level")
            new_geoscale_to_use = 'state'
            return new_geoscale_to_use
    if geoscale == 'state':
        fips = create_geoscale_list(df, 'county')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + ', '.join(activitynames) + "  at the county scale.")
        else:
            log.info("Flowbyactivity data exists for " + ', '.join(activitynames) + " at the county level")
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
        log.warning("LocationSystems do not match")


def check_if_data_exists_for_same_geoscales(fba_wsec_walloc, source, activity):  #fba_w_aggregated_sectors
    """
    Determine if data exists at the same scales for datasource and allocation source
    :param source_fba:
    :param allocation_fba:
    :return:
    """


    from flowsa.mapping import get_activitytosector_mapping

    # create list of highest sector level for which there should be data
    mapping = get_activitytosector_mapping(source)
    # filter by activity of interest
    mapping = mapping.loc[mapping['Activity'].isin(activity)]
    # add sectos to list
    sectors_list = pd.unique(mapping['Sector']).tolist()

    # subset fba w sectors and with merged allocation table so only have rows with aggregated sector list
    df_subset = fba_wsec_walloc.loc[
        (fba_wsec_walloc[fbs_activity_fields[0]].isin(sectors_list)) |
        (fba_wsec_walloc[fbs_activity_fields[1]].isin(sectors_list))].reset_index(drop=True)
    # only interested in total flows
    df_subset = df_subset.loc[df_subset['FlowName'] == 'total'].reset_index(drop=True)
    df_subset = df_subset.loc[df_subset['Compartment'] == 'total'].reset_index(drop=True)

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
    df['FlowAmount'] = np.where(df['Unit'] == 'ACRES', df['FlowAmount'] * 4046.8564224, df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'ACRES', 'm2.yr', df['Unit'])

    # class = money, unit = USD/yr

    # class = water, unit = m3/yr
    df['FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day', (df['FlowAmount'] / 264.172052) * 365,
                                df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'm3.p.yr', df['Unit'])

    df['FlowAmount'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', (df['FlowAmount'] / 4046.856422) * 1233.481837,
                                df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', 'm3.m2.yr', df['Unit'])

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
    group_cols = [e for e in group_cols if e not in ('ActivityProducedBy', 'ActivityConsumedBy', 'FlowName')]
    group_cols.append('Sector')

    # run sector aggregation fxn to determine total flowamount for each level of sector
    df_w_sectors = sector_aggregation_generalized(df_w_sectors, group_cols)

    # if statements for method of allocation
    if allocation_method == 'proportional':
        # denominator summed from highest level of sector grouped by location
        denom_df = df_w_sectors.loc[df_w_sectors['Sector'].apply(lambda x: len(x) == 2)]
        denom_df['Denominator'] = denom_df['FlowAmount'].groupby(denom_df['Location']).transform('sum')
        denom_df = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
        # merge the denominator column with fba_w_sector df
        allocation_df = df_w_sectors.merge(denom_df, how='left')
        # calculate ratio
        allocation_df['FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df['Denominator']
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
    helper_allocation = flowsa.getFlowByActivity(flowclass=[attr['helper_source_class']],
                                                 datasource=attr['helper_source'],
                                                 years=[attr['helper_source_year']])
    # fill null values
    helper_allocation = helper_allocation.fillna(value=fba_fill_na_dict)
    # convert unit
    helper_allocation = harmonize_units(helper_allocation)

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
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Sector', 'HelperFlow']], how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'county'):
        helper_allocation['Location_tmp'] = helper_allocation['Location'].apply(lambda x: str(x[0:2]))
        df_w_sector['Location_tmp'] = df_w_sector['Location'].apply(lambda x: str(x[0:2]))
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Sector', 'Location_tmp', 'HelperFlow']],
                                                    how='left')
        modified_fba_allocation = modified_fba_allocation.drop(columns=['Location_tmp'])

    # modify flow amounts using helper data
    if attr['helper_method'] == 'multiplication':
        modified_fba_allocation['FlowAmount'] = modified_fba_allocation['FlowAmount'] * modified_fba_allocation[
            'HelperFlow']
    # drop columns
    modified_fba_allocation = modified_fba_allocation.drop(columns="HelperFlow")

    return modified_fba_allocation


def sector_aggregation_generalized(fbs_df, group_cols):

    # drop any columns that contain a "-" in sector column
    fbs_df = fbs_df[~fbs_df['Sector'].str.contains('-', regex=True)]

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



def sector_aggregation(df, group_cols):
    """
    Function that checks if a sector length exists, and if not, sums the less aggregated sector
    :param df: Either a flowbyactivity df with sectors or a flowbysector df
    :param group_cols: columns by which to aggregate
    :return:
    """

    # testing
    # df = fbs.copy()
    # group_cols = fbs_default_grouping_fields.copy()

    # drop any columns that contain a "-" in sector column
    df = df[(~df[fbs_activity_fields[0]].str.contains('-', regex=True)) |
            (~df[fbs_activity_fields[1]].str.contains('-', regex=True))]

    # find the longest length sector
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(lambda x: x.str.len()).max().max()
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 2 > len(x) >= i) |
                           df[fbs_activity_fields[1]].apply(lambda x: i + 2 > len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_subset = df_subset[[fbs_activity_fields[0], fbs_activity_fields[1]]].drop_duplicates().reset_index(drop=True)
        df_sectors = sector_subset.copy()
        for col in df_sectors:
            df_sectors[col] = df_sectors[col].apply(lambda x: x[0:i])
        sector_list = df_sectors.drop_duplicates().values.tolist()
        # create a list of sectors that are exactly i digits long
        df_existing = sector_subset.copy()
        for col in df_existing:
            df_existing[col] = df_existing.loc[df_existing[col].apply(lambda x: len(x) == i)]
        existing_sectors = df_existing.drop_duplicates().dropna().values.tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = [e for e in sector_list if e not in existing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            agg_sectors_list = []
            for x, y in missing_sectors:
                c1 = df_subset[fbs_activity_fields[0]].str.startswith(x)
                c2 = df_subset[fbs_activity_fields[1]].str.startswith(y)
                # subset data
                agg_sectors_list.append(df_subset.loc[c1 & c2])
            agg_sectors = pd.concat(agg_sectors_list)
            agg_sectors = agg_sectors.loc[agg_sectors[fbs_activity_fields[0]].apply(lambda x: len(x) > i)]
            agg_sectors[fbs_activity_fields[0]] = agg_sectors[fbs_activity_fields[0]].apply(lambda x: str(x[0:i]))
            agg_sectors[fbs_activity_fields[1]] = agg_sectors[fbs_activity_fields[1]].apply(lambda x: str(x[0:i]))
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            df = df.append(agg_sectors).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df['SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['F0', 'F01']),
                                      'F010', df['SectorConsumedBy'])  # domestic/household
    df['SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['F0', 'F01']),
                                      'F010', df['SectorProducedBy'])  # domestic/household
    df['SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['No', 'Non']),
                                      'None', df['SectorConsumedBy'])  # domestic/household
    df['SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['No', 'Non']),
                                      'None', df['SectorProducedBy'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()
    df = df.sort_values(['SectorConsumedBy', 'SectorProducedBy']).reset_index(drop=True)

    return df






# def sector_aggregation(df_w_sectors, group_cols):
#     """
#     Function that checks if a sector length exists, and if not, sums the less aggregated sector
#     :param df_w_sectors: Either a flowbyactivity df with sectors or a flowbysector df
#     :param group_cols: columns by which to aggregate
#     :return:
#     """
#     # testing
#     # df_w_sectors = fbs.copy()
#     # group_cols = fbs_default_grouping_fields.copy()
#
#     # subset df into three df based on values in sector columns
#     # df 1 where sector produced by = none
#     df1 = df_w_sectors.loc[df_w_sectors['SectorProducedBy'] == 'None']
#     # df 2 where sector consumed by = none
#     df2 = df_w_sectors.loc[df_w_sectors['SectorConsumedBy'] == 'None']
#     # df 3 where values in both sector columns
#     df3 = df_w_sectors.loc[
#         (df_w_sectors['SectorProducedBy'] != 'None') & (df_w_sectors['SectorConsumedBy'] != 'None')]
#
#     fbs_dfs = []
#     for df in (df1, df2, df3):
#         # if the dataframe is not empty, run through sector aggregation code
#         if len(df) != 0:
#             if (df['SectorProducedBy'].all() == 'None') | (df['SectorConsumedBy'].all() == 'None'):
#                 if df['SectorProducedBy'].all() == 'None':
#                     sector = 'SectorConsumedBy'
#                 elif df['SectorConsumedBy'].all() == 'None':
#                     sector = 'SectorProducedBy'
#
#                 # drop any columns that contain a "-" in sector column
#                 df = df[~df[sector].str.contains('-', regex=True)]
#
#                 # find the longest length naics (will be 6 or 8)
#                 length = max(df[sector].apply(lambda x: len(x)).unique())
#                 # for loop in reverse order longest length naics minus 1 to 2
#                 # appends missing naics levels to df
#                 for i in range(length - 1, 1, -1):
#                     # subset df to sectors with length = i and length = i + 1
#                     df_subset = df.loc[df[sector].apply(lambda x: i + 2 > len(x) >= i)]
#                     # create a list of i digit sectors in df subset
#                     sector_list = df_subset[sector].apply(lambda x: str(x[0:i])).unique().tolist()
#                     # create a list of sectors that are exactly i digits long
#                     existing_sectors = df_subset[sector].loc[
#                         df_subset[sector].apply(lambda x: len(x) == i)].unique().tolist()
#                     # list of sectors of length i that are not in sector list
#                     missing_sectors = np.setdiff1d(sector_list, existing_sectors).tolist()
#                     # add start of symbol to missing list
#                     missing_sectors = ["^" + e for e in missing_sectors]
#                     if len(missing_sectors) != 0:
#                         # new df of sectors that start with missing sectors. drop the last digit of the sector and sum flows
#                         agg_sectors = df_subset.loc[df_subset[sector].str.contains('|'.join(missing_sectors))]
#                         # only keep data with length greater than i
#                         agg_sectors = agg_sectors.loc[agg_sectors[sector].apply(lambda x: len(x) > i)]
#                         agg_sectors[sector] = agg_sectors[sector].apply(lambda x: str(x[0:i]))
#                         agg_sectors = agg_sectors.fillna(0).reset_index()
#                         # aggregate the new sector flow amounts
#                         agg_sectors = aggregator(agg_sectors, group_cols)
#                         agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
#                         # append to df
#                         df = df.append(agg_sectors)
#                 fbs_dfs.append(df)
#             # if there are values in both sector columns...
#             else:
#                 # find the longest length naics (will be 6 or 8)
#                 length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(lambda x: x.str.len()).max().max()
#                 # for loop in reverse order longest length naics minus 1 to 2
#                 # appends missing naics levels to df
#                 for i in range(length - 1, 1, -1):
#                     # subset df to sectors with length = i and length = i + 1
#                     #todo: modify so based on both fbs columns and output warning if any data lost because mismatched sector length
#                     df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 2 > len(x) >= i)]
#                     # create a list of i digit sectors in df subset
#                     sector_subset = df_subset[[fbs_activity_fields[0], fbs_activity_fields[1]]].drop_duplicates().reset_index(drop=True)
#                     df_sectors = sector_subset.copy()
#                     for col in df_sectors:
#                         df_sectors[col] = df_sectors[col].apply(lambda x: x[0:i])
#                     sector_list = df_sectors.drop_duplicates().values.tolist()
#                     # create a list of sectors that are exactly i digits long
#                     df_existing = sector_subset.copy()
#                     for col in df_existing:
#                         df_existing[col] = df_existing.loc[df_existing[col].apply(lambda x: len(x) == i)]
#                     existing_sectors = df_existing.drop_duplicates().dropna().values.tolist()
#                     # list of sectors of length i that are not in sector list
#                     missing_sectors = [e for e in sector_list if e not in existing_sectors]
#                     if len(missing_sectors) != 0:
#                         # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
#                         # set conditions
#                         agg_sectors_list = []
#                         for x,y in missing_sectors:
#                             c1 = df_subset[fbs_activity_fields[0]].str.startswith(x)
#                             c2 = df_subset[fbs_activity_fields[1]].str.startswith(y)
#                             # subset data
#                             agg_sectors_list.append(df_subset.loc[c1 & c2])
#                         agg_sectors = pd.concat(agg_sectors_list)
#                         agg_sectors = agg_sectors.loc[agg_sectors[fbs_activity_fields[0]].apply(lambda x: len(x) > i)]
#                         agg_sectors[fbs_activity_fields[0]] = agg_sectors[fbs_activity_fields[0]].apply(lambda x: str(x[0:i]))
#                         agg_sectors[fbs_activity_fields[1]] = agg_sectors[fbs_activity_fields[1]].apply(lambda x: str(x[0:i]))
#                         agg_sectors = agg_sectors.fillna(0).reset_index()
#                         # aggregate the new sector flow amounts
#                         agg_sectors = aggregator(agg_sectors, group_cols)
#                         agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
#                         # append to df
#                         df = df.append(agg_sectors)
#                 fbs_dfs.append(df)
#
#     # concat and sort df
#     sector_agg_df = pd.concat(fbs_dfs, sort=True)
#     # manually modify non-NAICS codes that might exist in sector
#     sector_agg_df['SectorConsumedBy'] = np.where(sector_agg_df['SectorConsumedBy'].isin(['F0', 'F01']),
#                                                  'F010', sector_agg_df['SectorConsumedBy'])  # domestic/household
#     sector_agg_df['SectorProducedBy'] = np.where(sector_agg_df['SectorProducedBy'].isin(['F0', 'F01']),
#                                                  'F010', sector_agg_df['SectorProducedBy'])  # domestic/household
#     # drop any duplicates created by modifying sector codes
#     sector_agg_df = sector_agg_df.drop_duplicates()
#     sector_agg_df = sector_agg_df.sort_values(['SectorConsumedBy', 'SectorProducedBy']).reset_index(drop=True)
#
#     return sector_agg_df

