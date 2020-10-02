"""
Helper functions for flowbyactivity and flowbysector data
"""

import flowsa
import pandas as pd
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, flow_by_sector_fields, flow_by_sector_collapsed_fields, get_flow_by_groupby_cols, \
    create_fill_na_dict, generalize_activity_field_names, fips_number_key, \
    load_sector_length_crosswalk_w_nonnaics, update_geoscale, flow_by_activity_wsec_mapped_fields

fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)
fbs_collapsed_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_collapsed_fields)
fba_mapped_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_wsec_mapped_fields)


def clean_df(df, flowbyfields, fill_na_dict):
    """

    :param df:
    :param flowbyfields: flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: fba_fill_na_dict or fbs_fill_na_dict
    :return:
    """

    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # fill null values
    df = df.fillna(value=fill_na_dict)
    # drop description field, if exists
    if 'Description' in df.columns:
        df = df.drop(columns='Description')
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
    if geoscale == "national":
        fips.append(US_FIPS)
    elif df['LocationSystem'].str.contains('FIPS').any():
        # all_FIPS = read_stored_FIPS()
        if geoscale == "state":
            state_FIPS = get_state_FIPS(year)
            fips = list(state_FIPS['FIPS'])
        elif geoscale == "county":
            county_FIPS = get_county_FIPS(year)
            fips = list(county_FIPS['FIPS'])

    return fips


def filter_by_geoscale(df, geoscale):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param df: Either flowbyactivity or flowbysector
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity or flowbysector
    """

    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.error("No flows found in the "  + " flow dataset at the " + geoscale + " scale.")
    else:
        return df


def agg_by_geoscale(df, from_scale, to_scale, groupbycols):
    """

    :param df: flowbyactivity or flowbysector df
    :param from_scale:
    :param to_scale:
    :param groupbycolumns: flowbyactivity or flowbysector default groupby columns
    :return:
    """

    # use from scale to filter by these values
    df = filter_by_geoscale(df, from_scale).reset_index(drop=True)

    df = update_geoscale(df, to_scale)

    fba_agg = aggregator(df, groupbycols)

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

    # drop columns with flowamount = 0
    df = df[df['FlowAmount'] != 0]

    # aggregate df by groupby columns, either summing or creating weighted averages
    df_dfg = df.groupby(groupbycols, as_index=False).agg(agg_funx)

    df_dfg = df_dfg.replace({np.nan: None})

    return df_dfg


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered df
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields, flow_by_sector_fields, or flow_by_sector_collapsed_fields
    :return:
    """
    for k in flowbyfields.keys():
        if k not in flowby_partial_df.columns:
            flowby_partial_df[k] = None
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flowbyfields.items():
        flowby_partial_df.loc[:, k] = flowby_partial_df[k].astype(v[0]['dtype'])
    # Resort it so order is correct
    flowby_partial_df = flowby_partial_df[flowbyfields.keys()]
    return flowby_partial_df


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
    df = sector_disaggregation_generalized(df, group_cols)

    # if statements for method of allocation
    if allocation_method == 'proportional':
        allocation_df = proportional_allocation_by_location(df, 'Sector')

        return allocation_df


def proportional_allocation_by_location(df, sectorcolumn):
    """
    Creates a proportional allocation based on all the most aggregated sectors within a location
    :param df:
    :param sectorcolumn:
    :return:
    """

    denom_df = df.loc[df[sectorcolumn].apply(lambda x: len(x) == 2)]
    denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount'].groupby(
        denom_df['Location']).transform('sum')
    denom_df_2 = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2, how='left')
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
        'Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

    return allocation_df


def proportional_allocation_by_location_and_sector(df, sectorcolumn, level_of_aggregation):
    """
    Creates a proportional allocation within each aggregated sector within a location
    :param df:
    :param sectorcolumn:
    :param level_of_aggregation: 'agg' or 'disagg'
    :return:
    """

    # denominator summed from highest level of sector grouped by location
    short_length = min(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
    # want to create denominator based on short_length - 1, unless short_length = 2
    denom_df = df.loc[df[sectorcolumn].apply(lambda x: len(x) == short_length)]
    if (level_of_aggregation == 'agg') & (short_length != 2):
        short_length = short_length - 1
        denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn].apply(lambda x: x[0:short_length])
        denom_df.loc[:, 'Denominator'] = denom_df.groupby(['Location', 'sec_tmp'])['FlowAmount'].transform('sum')
    else:  # short_length == 2:]
        denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount']
        denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn]
    # if short_length == 2:
    #     denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount']
    #     denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn]
    # else:
    #     short_length = short_length - 1
    #     denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn].apply(lambda x: x[0:short_length])
    #     denom_df.loc[:, 'Denominator'] = denom_df.groupby(['Location', 'sec_tmp'])['FlowAmount'].transform('sum')

    denom_df_2 = denom_df[['Location', 'LocationSystem', 'Year', 'sec_tmp', 'Denominator']].drop_duplicates()
    # merge the denominator column with fba_w_sector df
    df.loc[:, 'sec_tmp'] = df[sectorcolumn].apply(lambda x: x[0:short_length])
    allocation_df = df.merge(denom_df_2, how='left', left_on=['Location', 'LocationSystem', 'Year', 'sec_tmp'],
                             right_on=['Location', 'LocationSystem', 'Year', 'sec_tmp'])
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
        'Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator', 'sec_tmp']).reset_index(drop=True)

    return allocation_df


def sector_ratios(df, sectorcolumn):
    """
    Determine ratios of the less aggregated sectors within a more aggregated sector
    :param df: A df with sector columns
    :param sectorcolumn: 'SectorConsumedBy' or 'SectorProducedBy'
    :return:
    """

    # drop any null rows (can occur when activities are ranges)
    df = df[~df[sectorcolumn].isnull()]

    # find the longest length sector
    length = max(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    sector_ratios = []
    for i in range(length, 3, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[sectorcolumn].apply(lambda x: len(x) == i)]
        # create column for sector grouping
        df_subset.loc[:, 'Sector_group'] = df_subset[sectorcolumn].apply(lambda x: x[0:i-1])
        # subset df to create denominator
        df_denom = df_subset[['FlowAmount', 'Location', 'Sector_group']]
        df_denom = df_denom.groupby(['Location', 'Sector_group'], as_index=False)[["FlowAmount"]].agg("sum")
        df_denom = df_denom.rename(columns={"FlowAmount": "Denominator"})
        # merge the denominator column with fba_w_sector df
        ratio_df = df_subset.merge(df_denom, how='left')
        # calculate ratio
        ratio_df.loc[:, 'FlowAmountRatio'] = ratio_df['FlowAmount'] / ratio_df['Denominator']
        ratio_df = ratio_df.drop(columns=['Denominator', 'Sector_group']).reset_index()
        sector_ratios.append(ratio_df)
    # concat list of dataframes (info on each page)
    df_w_ratios = pd.concat(sector_ratios, sort=True).reset_index(drop=True)

    return df_w_ratios


def allocation_helper(df_w_sector, method, attr):
    """
    Used when two df required to create allocation ratio
    :param df_w_sector:
    :param method: currently written for 'multiplication' and 'proportional'
    :param attr:
    :return:
    """

    from flowsa.Blackhurst_IO import scale_blackhurst_results_to_usgs_values
    from flowsa.BLS_QCEW import clean_bls_qcew_fba, bls_clean_allocation_fba_w_sec
    from flowsa.mapping import add_sectors_to_flowbyactivity

    helper_allocation = flowsa.getFlowByActivity(flowclass=[attr['helper_source_class']],
                                                 datasource=attr['helper_source'],
                                                 years=[attr['helper_source_year']])
    if 'clean_helper_fba' in attr:
        log.info("Cleaning " + attr['helper_source'] + ' FBA')
        # tmp hard coded - need to generalize
        if attr['helper_source'] == 'BLS_QCEW':
            helper_allocation = clean_bls_qcew_fba(helper_allocation, attr)
            # helper_allocation = getattr(sys.modules[__name__], attr["clean_helper_fba"])(helper_allocation, attr)
    # clean df
    helper_allocation = clean_df(helper_allocation, flow_by_activity_fields, fba_fill_na_dict)
    # drop rows with flowamount = 0
    helper_allocation = helper_allocation[helper_allocation['FlowAmount'] != 0]

    # filter geoscale
    helper_allocation = filter_by_geoscale(helper_allocation, attr['helper_from_scale'])

    # agg data if necessary
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'national'):
        helper_allocation = agg_by_geoscale(helper_allocation, 'state', 'national', fba_default_grouping_fields)


    # assign naics to allocation dataset
    helper_allocation = add_sectors_to_flowbyactivity(helper_allocation,
                                                      sectorsourcename=method['target_sector_source'],
                                                      levelofSectoragg=attr[
                                                          'helper_sector_aggregation'])

    # generalize activity field names to enable link to water withdrawal table
    helper_allocation = generalize_activity_field_names(helper_allocation)
    # clean up helper fba with sec
    if 'clean_helper_fba_wsec' in attr:
        log.info("Cleaning " + attr['helper_source'] + ' FBA with sectors')
        # tmp hard coded - need to generalize
        if attr['helper_source'] == 'BLS_QCEW':
            helper_allocation = bls_clean_allocation_fba_w_sec(helper_allocation, attr, method)
            # helper_allocation = getattr(sys.modules[__name__], attr["clean_helper_fba_wsec"])(helper_allocation, attr, method)
    # drop columns
    helper_allocation = helper_allocation.drop(columns=['Activity', 'Min', 'Max'])

    if attr['helper_method'] == 'proportional':
        # if calculating proportion, first subset the helper allocation df to only contain relevant sectors
        # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
        # aren't in list
        sector_list = df_w_sector['Sector'].unique().tolist()
        # subset fba allocation table to the values in the activity list, based on overlapping sectors
        helper_allocation = helper_allocation.loc[helper_allocation['Sector'].isin(sector_list)]
        # calculate proportional ratios
        helper_allocation = proportional_allocation_by_location_and_sector(helper_allocation, 'Sector',
                                                                           attr['allocation_sector_aggregation'])

    # rename column
    helper_allocation = helper_allocation.rename(columns={"FlowAmount": 'HelperFlow'})
    merge_columns = [e for e in ['Location','Sector', 'HelperFlow', 'FlowAmountRatio'] if e in
                     helper_allocation.columns.values.tolist()]
    # merge allocation df with helper df based on sectors, depending on geo scales of dfs
    if attr['helper_from_scale'] == 'national':
        modified_fba_allocation = df_w_sector.merge(helper_allocation[merge_columns], how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'state'):
        modified_fba_allocation = df_w_sector.merge(helper_allocation[merge_columns], how='left')
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'county'):
        helper_allocation.loc[:, 'Location_tmp'] = helper_allocation['Location'].apply(lambda x: x[0:2])
        df_w_sector.loc[:, 'Location_tmp'] = df_w_sector['Location'].apply(lambda x: x[0:2])
        merge_columns.append('Location_tmp')
        modified_fba_allocation = df_w_sector.merge(helper_allocation[merge_columns], how='left')
        modified_fba_allocation = modified_fba_allocation.drop(columns=['Location_tmp'])
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'national'):
        modified_fba_allocation = df_w_sector.merge(helper_allocation[merge_columns], how='left')

    # modify flow amounts using helper data
    if 'multiplication' in attr['helper_method']:
        # todo: modify so if missing data, replaced with value from one geoscale up instead of national
        # todo: modify year after merge if necessary
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

        # replace non-existent helper flow values with a 0, so after multiplying, don't have incorrect value associated
        # with new unit
        modified_fba_allocation['HelperFlow'] = modified_fba_allocation['HelperFlow'].fillna(value=0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = modified_fba_allocation['FlowAmount'] * \
                                                       modified_fba_allocation['HelperFlow']
        # drop columns
        modified_fba_allocation = modified_fba_allocation.drop(columns=["HelperFlow", 'ReplacementValue'])

    elif attr['helper_method'] == 'proportional':
        modified_fba_allocation['FlowAmountRatio'] = modified_fba_allocation['FlowAmountRatio'].fillna(0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = modified_fba_allocation['FlowAmount'] * \
                                                       modified_fba_allocation['FlowAmountRatio']
        modified_fba_allocation = modified_fba_allocation.drop(columns=["HelperFlow", 'FlowAmountRatio'])

    # drop rows of 0
    modified_fba_allocation = modified_fba_allocation[modified_fba_allocation['FlowAmount'] != 0].reset_index(drop=True)

    # todo: change units
    modified_fba_allocation.loc[modified_fba_allocation['Unit'] == 'gal/employee', 'Unit'] = 'gal'

    # option to scale up fba values
    if 'scaled' in attr['helper_method']:
        log.info("Scaling " + attr['helper_source'] + ' to FBA values')
        # tmp hard coded - need to generalize
        if attr['helper_source'] == 'BLS_QCEW':
            modified_fba_allocation = scale_blackhurst_results_to_usgs_values(modified_fba_allocation, attr)
            # modified_fba_allocation = getattr(sys.modules[__name__], attr["scale_helper_results"])(modified_fba_allocation, attr)

    return modified_fba_allocation


def sector_aggregation_generalized(df, group_cols):
    """
    If a sector value is not included in df, sum together less aggregated sectors to calculate value.
    This function works for df with one sector column called "Sector"
    :param df: A df with a 'Sector' column
    :param group_cols: columns to group aggregation by
    :return: A df with sector levels summed from the least aggregated level
    """


    # ensure None values are not strings
    df['Sector'] = df['Sector'].replace({'nan': ""})
    df['Sector'] = df['Sector'].replace({'None': ""})

    # find the longest length sector
    length = max(df['Sector'].apply(lambda x: len(x)).unique())
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df['Sector'].apply(lambda x: i + 1 >= len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_subset = df_subset[['Location', 'Sector']].drop_duplicates().reset_index(drop=True)
        df_sectors = sector_subset.copy()
        df_sectors.loc[:, 'Sector'] = df_sectors['Sector'].apply(lambda x: x[0:i])
        sector_list = df_sectors.drop_duplicates().values.tolist()
        # create a list of sectors that are exactly i digits long
        # where either sector column is i digits in length
        df_existing = sector_subset.loc[(sector_subset['Sector'].apply(lambda x: len(x) == i))]
        existing_sectors = df_existing.drop_duplicates().dropna().values.tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = [e for e in sector_list if e not in existing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            agg_sectors_list = []
            for q, r in missing_sectors:
                c1 = df_subset['Location'] == q
                c2 = df_subset['Sector'].apply(lambda x: x[0:i] == r)
                # subset data
                agg_sectors_list.append(df_subset.loc[c1 & c2])
            agg_sectors = pd.concat(agg_sectors_list, sort=False)
            agg_sectors = agg_sectors.loc[
                (agg_sectors['Sector'].apply(lambda x: len(x) > i))]
            agg_sectors.loc[:, 'Sector'] = agg_sectors['Sector'].apply(lambda x: x[0:i])
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors2 = aggregator(agg_sectors, group_cols)
            agg_sectors2 = agg_sectors2.fillna(0).reset_index(drop=True)
            # append to df
            agg_sectors2['Sector'] = agg_sectors2['Sector'].replace({'nan': ""})
            df = df.append(agg_sectors2, sort=False).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df.loc[:, 'Sector'] = np.where(df['Sector'].isin(['F0', 'F01']),
                                   'F010', df['Sector'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()
    # replace null values
    df = df.replace({'': None})

    return df


def sector_aggregation(df, group_cols):
    """
    Function that checks if a sector length exists, and if not, sums the less aggregated sector
    :param df: Either a flowbyactivity df with sectors or a flowbysector df
    :param group_cols: columns by which to aggregate
    :return:
    """

    # ensure None values are not strings
    df['SectorConsumedBy'] = df['SectorConsumedBy'].replace({'nan': ""})
    df['SectorProducedBy'] = df['SectorProducedBy'].replace({'nan': ""})
    df['SectorConsumedBy'] = df['SectorConsumedBy'].replace({'None': ""})
    df['SectorProducedBy'] = df['SectorProducedBy'].replace({'None': ""})

    # find the longest length sector
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).max().max()
    length = int(length)
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
        # where both sector columns are i digits in length
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
            for q, r, s in missing_sectors:
                c1 = df_subset['Location'] == q
                c2 = df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i] == r)     #.str.startswith(y)
                c3 = df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i] == s)   #.str.startswith(z)
                # subset data
                agg_sectors_list.append(df_subset.loc[c1 & c2 & c3])
            agg_sectors = pd.concat(agg_sectors_list, sort=False)
            agg_sectors = agg_sectors.loc[
                (agg_sectors[fbs_activity_fields[0]].apply(lambda x: len(x) > i)) |
                (agg_sectors[fbs_activity_fields[1]].apply(lambda x: len(x) > i))]
            agg_sectors.loc[:, fbs_activity_fields[0]] = agg_sectors[fbs_activity_fields[0]].apply(
                lambda x: x[0:i])
            agg_sectors.loc[:, fbs_activity_fields[1]] = agg_sectors[fbs_activity_fields[1]].apply(
                lambda x: x[0:i])
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            agg_sectors['SectorConsumedBy'] = agg_sectors['SectorConsumedBy'].replace({'nan': ""})
            agg_sectors['SectorProducedBy'] = agg_sectors['SectorProducedBy'].replace({'nan': ""})
            df = df.append(agg_sectors, sort=False).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df.loc[:, 'SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorConsumedBy'])  # domestic/household
    df.loc[:, 'SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorProducedBy'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()
    # replace null values
    df = df.replace({'': None})

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


    sector_disaggregation = clean_df(sector_disaggregation, flow_by_sector_fields, fbs_fill_na_dict)

    # ensure None values are not strings
    sector_disaggregation['SectorConsumedBy'] = sector_disaggregation['SectorConsumedBy'].replace({'None': ""})
    sector_disaggregation['SectorProducedBy'] = sector_disaggregation['SectorProducedBy'].replace({'None': ""})

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk_w_nonnaics()
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
        df_subset = sector_disaggregation.loc[sector_disaggregation[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                                              sector_disaggregation[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset.loc[:, 'SectorProduced_tmp'] = df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i])
        df_subset.loc[:, 'SectorConsumed_tmp'] = df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i])
        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset_1 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) &
                                    (df_subset['SectorConsumed_tmp'] == "")]
        df_subset_2 = df_subset.loc[(df_subset['SectorProduced_tmp'] == "") &
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        df_subset_3 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) &
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        # concat existing dfs
        df_subset = pd.concat([df_subset_1, df_subset_2, df_subset_3], sort=False)
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
        new_naics['SectorConsumedBy'] = new_naics['SectorConsumedBy'].replace({'nan': ""})
        new_naics['SectorProducedBy'] = new_naics['SectorProducedBy'].replace({'nan': ""})
        sector_disaggregation = pd.concat([sector_disaggregation, new_naics], sort=True)
    # replace blank strings with None
    sector_disaggregation = sector_disaggregation.replace({'': None})
    sector_disaggregation = sector_disaggregation.replace({np.nan: None})

    return sector_disaggregation


def sector_disaggregation_generalized(fbs, group_cols):
    """
    function to disaggregate sectors if there is only one naics at a lower level
    works for lower than naics 4
    :param df: A FBS df
    :return: A FBS df with missing naics5 and naics6
    """

    # test
    # fbs = naics2.copy()

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk_w_nonnaics()
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

        # test
        # i = 4

        if i == 4:
            sector_list = naics4
            sector_merge = "NAICS_4"
            sector_add = "NAICS_5"
        elif i == 5:
            sector_list = naics5
            sector_merge = "NAICS_5"
            sector_add = "NAICS_6"

        # subset df to sectors with length = i and length = i + 1
        df_subset = fbs[fbs['Sector'].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset.loc[:, 'Sector_tmp'] = df_subset['Sector'].apply(lambda x: x[0:i])
        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset = df_subset.loc[df_subset['Sector_tmp'].isin(sector_list)]
        # drop all rows with duplicate temp values, as a less aggregated naics exists
        group_cols = [e for e in group_cols if e not in ('Sector')]
        group_cols.append('Sector_tmp')
        df_subset = df_subset.drop_duplicates(subset=group_cols,
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

    # ensure correct datatypes and order
    fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)

    # collapse the FBS sector columns into one column based on FlowType
    fbs.loc[fbs["FlowType"] == 'TECHNOSPHERE_FLOW', 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[fbs["FlowType"] == 'WASTE_FLOW', 'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'WASTE_FLOW') & (fbs['SectorProducedBy'] == 'None'), 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorProducedBy'] == 'None'), 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'] == 'None'), 'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'].isin(['F010', 'F0100', 'F01000'])) &
            (fbs['SectorProducedBy'].isin(['22', '221', '2213', '22131', '221310'])), 'Sector'] = fbs["SectorConsumedBy"]

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # reorder df columns and ensure correct datatype
    fbs_collapsed = add_missing_flow_by_fields(fbs_collapsed, flow_by_sector_collapsed_fields)
    # aggregate
    fbs_collapsed = aggregator(fbs_collapsed, fbs_collapsed_default_grouping_fields)
    # sort dataframe
    fbs_collapsed = fbs_collapsed.sort_values(
        ['Location', 'Flowable', 'Context', 'Sector']).reset_index(drop=True)

    return fbs_collapsed


def return_activity_from_scale(df, provided_from_scale):
    """
    Determine the 'from scale' used for aggregation/df subsetting for each activity combo in a df
    :param df: flowbyactivity df
    :param activity_df: a df with the activityproducedby, activityconsumedby columns and
    a column 'exists' denoting if data is available at specified geoscale
    :param provided_from_scale: The scale to use specified in method yaml
    :return:
    """

    # determine the unique combinations of activityproduced/consumedby
    unique_activities = unique_activity_names(df)
    # filter by geoscale
    fips = create_geoscale_list(df, provided_from_scale)
    df_sub = df[df['Location'].isin(fips)]
    # determine unique activities after subsetting by geoscale
    unique_activities_sub = unique_activity_names(df_sub)

    # return df of the difference between unique_activities and unique_activities2
    df_missing = dataframe_difference(unique_activities, unique_activities_sub, which='left_only')
    # return df of the similarities between unique_activities and unique_activities2
    df_existing = dataframe_difference(unique_activities, unique_activities_sub, which='both')
    df_existing = df_existing.drop(columns='_merge')
    df_existing['activity_from_scale'] = provided_from_scale

    # for loop through geoscales until find data for each activity combo
    if provided_from_scale == 'national':
        geoscales = ['state', 'county']
    elif provided_from_scale == 'state':
        geoscales = ['county']
    elif provided_from_scale == 'county':
        log.info('No data - skipping')

    if len(df_missing) > 0:
        for i in geoscales:
            # filter by geoscale
            fips_i = create_geoscale_list(df, i)
            df_i = df[df['Location'].isin(fips_i)]

            # determine unique activities after subsetting by geoscale
            unique_activities_i = unique_activity_names(df_i)

            # return df of the difference between unique_activities subset and unique_activities for geoscale
            df_missing_i = dataframe_difference(unique_activities_sub, unique_activities_i, which='right_only')
            df_missing_i = df_missing_i.drop(columns='_merge')
            df_missing_i['activity_from_scale'] = i
            # return df of the similarities between unique_activities and unique_activities2
            df_existing_i = dataframe_difference(unique_activities_sub, unique_activities_i, which='both')

            # append unique activities and df with defined activity_from_scale
            unique_activities_sub = unique_activities_sub.append(df_missing_i[[fba_activity_fields[0],
                                                                               fba_activity_fields[1]]])
            df_existing = df_existing.append(df_missing_i)
            df_missing = dataframe_difference(df_missing[[fba_activity_fields[0],fba_activity_fields[1]]],
                                              df_existing_i[[fba_activity_fields[0],fba_activity_fields[1]]],
                                              which=None)

    return df_existing


def subset_df_by_geoscale(df, activity_from_scale, activity_to_scale):
    """
    Subset a df by geoscale or agg to create data specified in method yaml
    :param flow_subset:
    :param activity_from_scale:
    :param activity_to_scale:
    :return:
    """

    # determine 'activity_from_scale' for use in df geoscale subset,  by activity
    log.info('Check if data exists at ' + activity_from_scale + ' level')
    modified_from_scale = return_activity_from_scale(df, activity_from_scale)
    # add 'activity_from_scale' column to df
    df2 = pd.merge(df, modified_from_scale)

    # list of unique 'from' geoscales
    unique_geoscales = modified_from_scale['activity_from_scale'].drop_duplicates().values.tolist()

    # to scale
    if fips_number_key[activity_from_scale] > fips_number_key[activity_to_scale]:
        to_scale = activity_to_scale
    else:
        to_scale = activity_from_scale

    df_subset_list = []
    # subset df based on activity 'from' scale
    for i in unique_geoscales:
        df3 = df2[df2['activity_from_scale'] == i]
        # if desired geoscale doesn't exist, aggregate existing data
        # if df is less aggregated than allocation df, aggregate fba activity to allocation geoscale
        if fips_number_key[i] > fips_number_key[to_scale]:
            log.info("Aggregating subset from " + i + " to " + to_scale)
            df_sub = agg_by_geoscale(df3, i, to_scale, fba_default_grouping_fields)
        # else filter relevant rows
        else:
            log.info("Subsetting " + i + " data")
            df_sub = filter_by_geoscale(df3, i)
        df_subset_list.append(df_sub)
    df_subset = pd.concat(df_subset_list)

    return df_subset


def unique_activity_names(fba_df):
    """
    Determine the unique activity names in a df
    :param fba_df: a flowbyactivity df
    :return: df with ActivityProducedBy and ActivityConsumedBy columns
    """

    activities = fba_df[[fba_activity_fields[0], fba_activity_fields[1]]]
    unique_activities = activities.drop_duplicates().reset_index(drop=True)

    return unique_activities


def dataframe_difference(df1, df2, which=None):
    """
    Find rows which are different between two DataFrames
    :param df1:
    :param df2:
    :param which: 'both', 'right_only', 'left_only'
    :return:
    """
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]

    return diff_df
