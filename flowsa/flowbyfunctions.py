"""
Helper functions for flowbyactivity and flowbysector data
"""

import pandas as pd
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields, flow_by_sector_fields, flow_by_sector_collapsed_fields, get_flow_by_groupby_cols, \
    create_fill_na_dict, fips_number_key, load_sector_length_crosswalk, update_geoscale, flow_by_activity_wsec_mapped_fields

fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]

fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)
fbs_collapsed_fill_na_dict = create_fill_na_dict(flow_by_sector_collapsed_fields)

fba_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)
fbs_grouping_fields_w_activities = fbs_default_grouping_fields + (['ActivityProducedBy', 'ActivityConsumedBy'])
fbs_collapsed_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_collapsed_fields)
fba_mapped_default_grouping_fields = get_flow_by_groupby_cols(flow_by_activity_wsec_mapped_fields)


def clean_df(df, flowbyfields, fill_na_dict, drop_description=True):
    """

    :param df:
    :param flowbyfields: flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: fba_fill_na_dict or fbs_fill_na_dict
    :param drop_description: specify if want the Description column dropped, defaults to true
    :return:
    """

    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # fill null values
    df = df.fillna(value=fill_na_dict)
    # drop description field, if exists
    if 'Description' in df.columns and drop_description is True:
        df = df.drop(columns='Description')
    if flowbyfields == 'flow_by_sector_fields':
        # harmonize units across dfs
        df = harmonize_units(df)
    # if datatypes are strings, ensure that Null values remain NoneType
    df = replace_strings_with_NoneType(df)

    return df


def replace_strings_with_NoneType(df):
    """
    Ensure that cell values in columns with datatype = string remain NoneType
    :param df: df with columns where datatype = object
    :return: A df where values are NoneType if they are supposed to be
    """
    # if datatypes are strings, ensure that Null values remain NoneType
    for y in df.columns:
        if df[y].dtype == object:
            df[y] = df[y].replace({'nan': None,
                                   'None': None,
                                   np.nan: None,
                                   '': None})
    return df


def replace_NoneType_with_empty_cells(df):
    """
    Replace all NoneType in columns where datatype = string with empty cells
    :param df: df with columns where datatype = object
    :return: A df where values are '' when previously they were NoneType
    """
    # if datatypes are strings, change NoneType to empty cells
    for y in df.columns:
        if df[y].dtype == object:
            df.loc[:, y] = df[y].replace({'nan': '',
                                          'None': '',
                                          np.nan: '',
                                          None: ''})
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

    df = df[df['Location'].isin(fips)].reset_index(drop=True)

    if len(df) == 0:
        log.error("No flows found in the " + " flow dataset at the " + geoscale + " scale")
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


def weighted_average(df, data_col, weight_col, by_col):
    """
    Generates a weighted average result based on passed columns
    Parameters
    ----------
    df : DataFrame
        Dataframe prior to aggregating from which a weighted average is calculated
    data_col : str
        Name of column to be averaged.
    weight_col : str
        Name of column to serve as the weighting.
    by_col : list
        List of columns on which the dataframe is aggregated.
    Returns
    -------
    result : series
        Series reflecting the weighted average values for the data_col,
        at length consistent with the aggregated dataframe, to be reapplied
        to the data_col in the aggregated dataframe.
    """

    df = df.assign(_data_times_weight=df[data_col] * df[weight_col])
    df = df.assign(_weight_where_notnull=df[weight_col] * pd.notnull(df[data_col]))
    g = df.groupby(by_col)
    result = g['_data_times_weight'].sum() / g['_weight_where_notnull'].sum()
    del df['_data_times_weight'], df['_weight_where_notnull']
    return result


def aggregator(df, groupbycols):
    """
    Aggregates flowbyactivity or flowbysector df by given groupbycols

    :param df: Either flowbyactivity or flowbysector
    :param groupbycols: Either flowbyactivity or flowbysector columns
    :return:
    """

    # tmp replace null values with empty cells
    df = replace_NoneType_with_empty_cells(df)

    # drop columns with flowamount = 0
    df = df[df['FlowAmount'] != 0]

    # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
    possible_column_headers = ('Spread', 'Min', 'Max', 'DataReliability', 'TemporalCorrelation',
                               'GeographicalCorrelation', 'TechnologicalCorrelation',
                               'DataCollection')

    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in df.columns.values.tolist()]

    df_dfg = df.groupby(groupbycols).agg({'FlowAmount': ['sum']})

    # run through other columns creating weighted average
    for e in column_headers:
        df_dfg[e] = weighted_average(df, e, 'FlowAmount', groupbycols)

    df_dfg = df_dfg.reset_index()
    df_dfg.columns = df_dfg.columns.droplevel(level=1)

    # if datatypes are strings, ensure that Null values remain NoneType
    df_dfg = replace_strings_with_NoneType(df_dfg)

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
    Timeframe is over one year
    :param df: Either flowbyactivity or flowbysector
    :return: Df with standarized units
    """

    days_in_year = 365
    sq_ft_to_sq_m_multiplier = 0.092903
    gallon_water_to_kg = 3.79  # rounded to match USGS_NWIS_WU mapping file on FEDEFL
    ac_ft_water_to_kg = 1233481.84
    acre_to_m2 = 4046.8564224

    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRES', df['FlowAmount'] * acre_to_m2,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRES', 'm2', df['Unit'])
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Acres', df['FlowAmount'] * acre_to_m2,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Acres', 'm2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']),
                                       df['FlowAmount'] * sq_ft_to_sq_m_multiplier * 1000000,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']), 'm2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['square feet']),
                                       df['FlowAmount'] * sq_ft_to_sq_m_multiplier,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['square feet']), 'm2', df['Unit'])

    # class = money, unit = USD

    # class = water, unit = kg
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day',
                                       (df['FlowAmount'] * gallon_water_to_kg) * days_in_year,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'kg', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRE FEET / ACRE',
                                       (df['FlowAmount'] / acre_to_m2) * ac_ft_water_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', 'kg/m2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Mgal',
                                       df['FlowAmount'] * 1000000 * gallon_water_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal', 'kg', df['Unit'])

    # class = other, unit varies

    return df


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
        df_subset = df_subset.assign(Sector_group=df_subset[sectorcolumn].apply(lambda x: x[0:i-1]))
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


def sector_aggregation(df, group_cols):
    """
    Function that checks if a sector length exists, and if not, sums the less aggregated sector
    :param df: Either a flowbyactivity df with sectors or a flowbysector df
    :param group_cols: columns by which to aggregate
    :return:
    """

    # todo: add statement that if sector-like, drop activity cols if exist (then add them back as copies of sector cols)
    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df)

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
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            # append to df
            agg_sectors = replace_NoneType_with_empty_cells(agg_sectors)
            # agg_sectors['SectorConsumedBy'] = agg_sectors['SectorConsumedBy'].replace({np.nan: ""})
            # agg_sectors['SectorProducedBy'] = agg_sectors['SectorProducedBy'].replace({np.nan: ""})
            df = df.append(agg_sectors, sort=False).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df.loc[:, 'SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorConsumedBy'])  # domestic/household
    df.loc[:, 'SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorProducedBy'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()
    # replace null values
    df = replace_strings_with_NoneType(df)

    return df


def sector_disaggregation(df, group_cols):
    """
    function to disaggregate sectors if there is only one naics at a lower level
    works for lower than naics 4
    :param df_load: A FBS df
    :param group_cols:
    :return: A FBS df with missing naics5 and naics6
    """

    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df)

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk()

    # for loop min length to 6 digits, where min length cannot be less than 2
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).min().min()
    if length < 2:
        length = 2
    # appends missing naics levels to df
    for i in range(length, 6):
        sector_merge = 'NAICS_' + str(i)
        sector_add = 'NAICS_' + str(i+1)

        # subset the df by naics length
        cw = cw_load[[sector_merge, sector_add]]
        # only keep the rows where there is only one value in sector_add for a value in sector_merge
        cw = cw.drop_duplicates(subset=[sector_merge], keep=False).reset_index(drop=True)
        sector_list = cw[sector_merge].values.tolist()

        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                           df[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset = df_subset.assign(SectorProduced_tmp=df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i]))
        df_subset = df_subset.assign(SectorConsumed_tmp=df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i]))
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
        # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
        possible_column_headers = ('Flowable', 'FlowName', 'Unit', 'Context', 'Compartment', 'Location', 'Year',
                                   'SectorProduced_tmp', 'SectorConsumed_tmp')
        # list of column headers that do exist in the df being subset
        cols_to_drop = [e for e in possible_column_headers if e in df_subset.columns.values.tolist()]

        df_subset = df_subset.drop_duplicates(subset=cols_to_drop, keep=False).reset_index(drop=True)

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
        new_naics['SectorConsumedBy'] = new_naics['SectorConsumedBy'].replace({np.nan: ""})
        new_naics['SectorProducedBy'] = new_naics['SectorProducedBy'].replace({np.nan: ""})
        new_naics = replace_NoneType_with_empty_cells(new_naics)
        df = pd.concat([df, new_naics], sort=True)
    # replace blank strings with None
    df = replace_strings_with_NoneType(df)

    return df


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
    fbs = clean_df(fbs, flow_by_sector_fields, fbs_fill_na_dict)

    # collapse the FBS sector columns into one column based on FlowType
    fbs.loc[fbs["FlowType"] == 'TECHNOSPHERE_FLOW', 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[fbs["FlowType"] == 'WASTE_FLOW', 'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'WASTE_FLOW') & (fbs['SectorProducedBy'].isnull()), 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorProducedBy'].isnull()), 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'].isnull()), 'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'].isin(['F010', 'F0100', 'F01000'])) &
            (fbs['SectorProducedBy'].isin(['22', '221', '2213', '22131', '221310'])), 'Sector'] = fbs["SectorConsumedBy"]

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # aggregate
    fbs_collapsed = aggregator(fbs_collapsed, fbs_collapsed_default_grouping_fields)
    # sort dataframe
    fbs_collapsed = clean_df(fbs_collapsed, flow_by_sector_collapsed_fields, fbs_collapsed_fill_na_dict)
    fbs_collapsed = fbs_collapsed.sort_values(['Sector', 'Flowable', 'Context', 'Location']).reset_index(drop=True)

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

    # method of subset dependent on LocationSystem
    if df['LocationSystem'].str.contains('FIPS').any():
        df = df[df['LocationSystem'].str.contains('FIPS')].reset_index(drop=True)
        # determine 'activity_from_scale' for use in df geoscale subset, by activity
        modified_from_scale = return_activity_from_scale(df, activity_from_scale)
        # add 'activity_from_scale' column to df
        df2 = pd.merge(df, modified_from_scale)

        # list of unique 'from' geoscales
        unique_geoscales = modified_from_scale['activity_from_scale'].drop_duplicates().values.tolist()
        if len(unique_geoscales) > 1:
            log.info('Dataframe has a mix of geographic levels: ' + ', '.join(unique_geoscales))

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
        df_subset = pd.concat(df_subset_list, ignore_index=True)

        # only keep cols associated with FBA
        df_subset = clean_df(df_subset, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)

    # right now, the only other location system is for Statistics Canada data
    else:
        df_subset = df.copy()

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


def estimate_suppressed_data(df, sector_column):
    """
    Estimate data suppressions
    :param df:
    :return:
    """

    # exclude nonsectors
    df = replace_NoneType_with_empty_cells(df)

    # can be changed to expand range - takes a long time and at national level, only missing suppresed \
    # 6 digit for industrial
    estimate_range = [5]
    for i in estimate_range:

        # create df of i length
        df_x = df.loc[df[sector_column].apply(lambda x: len(x) == i)]

        # create df of i + 1 length
        df_y = df.loc[df[sector_column].apply(lambda x: len(x) == i + 1)]

        # create temp sector columns in df y, that are i digits in length
        df_y = df_y.assign(s_tmp=df_y[sector_column].apply(lambda x: x[0:i]))

        # create list of location and temp activity combos that contain a 0
        missing_sectors_df = df_y[df_y['FlowAmount'] == 0]
        missing_sectors_list = missing_sectors_df[['Location', 's_tmp']].drop_duplicates().values.tolist()
        # subset the y df
        if len(missing_sectors_list) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            suppressed_list = []
            for q, r, in missing_sectors_list:
                c1 = df_y['Location'] == q
                c2 = df_y['s_tmp'] == r
                # subset data
                suppressed_list.append(df_y.loc[c1 & c2])
            suppressed_sectors = pd.concat(suppressed_list, sort=False)
            # add column of existing allocated data for length of i
            suppressed_sectors['alloc_flow'] = suppressed_sectors.groupby(['Location', 's_tmp'])['FlowAmount'].transform('sum')
            # subset further so only keep rows of 0 value
            suppressed_sectors_sub = suppressed_sectors[suppressed_sectors['FlowAmount'] == 0]
            # add count
            suppressed_sectors_sub = suppressed_sectors_sub.assign(sector_count=suppressed_sectors_sub.groupby(['Location', 's_tmp'])['s_tmp'].transform('count'))

            # merge suppressed sector subset with df x
            df_m = pd.merge(df_x,
                            suppressed_sectors_sub[['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                                    'Year', sector_column, 's_tmp', 'alloc_flow', 'sector_count']],
                            how='right',
                            left_on=['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                     'Year', sector_column],
                            right_on=['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                      'Year', 's_tmp'])
            # calculate estimated flows by subtracting the flow amount already allocated from total flow of \
            # sector one level up and divide by number of sectors with suppresed data
            df_m.loc[:, 'FlowAmount'] = (df_m['FlowAmount'] - df_m['alloc_flow']) / df_m['sector_count']
            # only keep the suppressed sector subset activity columns
            df_m = df_m.drop(columns=[sector_column + '_x', 's_tmp', 'alloc_flow', 'sector_count'])
            df_m = df_m.rename(columns={sector_column + '_y': sector_column})

            # drop the existing rows with suppressed data and append the new estimates from fba df
            modified_df = pd.merge(df, suppressed_sectors_sub, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
            df = pd.concat([modified_df, df_m], ignore_index=True, sort=True)

    df_w_estimated_data = replace_strings_with_NoneType(df)
    # drop cols
    # df_w_estimated_data = df_w_estimated_data.drop(columns=['s_tmp', 'alloc_flow', 'sector_count'])
    # reorder cols
    # df_w_estimated_data = df = add_missing_flow_by_fields(df_w_estimated_data, fba_mapped_default_grouping_fields)

    return df_w_estimated_data


def collapse_activity_fields(df):
    """
    The 'activityconsumedby' and 'activityproducedby' columns from the allocation dataset do not always align with
    the water use dataframe. Generalize the allocation activity column.
    :param fba_df:
    :return:
    """

    df = replace_strings_with_NoneType(df)

    activity_consumed_list = df['ActivityConsumedBy'].drop_duplicates().values.tolist()
    activity_produced_list = df['ActivityProducedBy'].drop_duplicates().values.tolist()

    # if an activity field column is all 'none', drop the column and rename renaming activity columns to generalize
    if all(v is None for v in activity_consumed_list):
        df = df.drop(columns=['ActivityConsumedBy', 'SectorConsumedBy'])
        df = df.rename(columns={'ActivityProducedBy': 'Activity',
                                'SectorProducedBy': 'Sector'})
    elif all(v is None for v in activity_produced_list):
        df = df.drop(columns=['ActivityProducedBy', 'SectorProducedBy'])
        df = df.rename(columns={'ActivityConsumedBy': 'Activity',
                                'SectorConsumedBy': 'Sector'})
    else:
        log.error('Cannot generalize dataframe')

    # drop other columns
    df = df.drop(columns=['ProducedBySectorType', 'ConsumedBySectorType'])

    return df


def harmonize_FBS_columns(df):
    """
    For FBS use in USEEIOR, harmonize the values in the columns
    - LocationSystem: drop the year, so just 'FIPS'
    - MeasureofSpread: tmp set to NoneType as values currently misleading
    - Spread: tmp set to 0 as values currently misleading
    - DistributionType: tmp set to NoneType as values currently misleading
    - MetaSources: Combine strings for rows where class/context/flowtype/flowable/etc. are equal
    :param df: FBS dataframe with mixed values/strings in columns
    :return: FBS df with harmonized values/strings in columns
    """

    # harmonize LocationSystem column
    log.info('Drop year in LocationSystem')
    if df['LocationSystem'].str.contains('FIPS').all():
        df = df.assign(LocationSystem='FIPS')
    # harmonize MeasureofSpread
    log.info('Reset MeasureofSpread to NoneType')
    df = df.assign(MeasureofSpread=None)
    # reset spread, as current values are misleading
    log.info('Reset Spread to 0')
    df = df.assign(Spread=0)
    # harmonize Distributiontype
    log.info('Reset DistributionType to NoneType')
    df = df.assign(DistributionType=None)

    # harmonize metasources
    log.info('Harmonize MetaSources')
    df = replace_NoneType_with_empty_cells(df)

    # subset all string cols of the df and drop duplicates
    string_cols = ['Flowable', 'Class', 'SectorProducedBy', 'SectorConsumedBy',  'SectorSourceName', 'Context',
                   'Location', 'LocationSystem', 'Unit', 'FlowType', 'Year', 'MeasureofSpread', 'MetaSources']
    df_sub = df[string_cols].drop_duplicates().reset_index(drop=True)
    # sort df
    df_sub = df_sub.sort_values(['MetaSources', 'SectorProducedBy', 'SectorConsumedBy']).reset_index(drop=True)

    # new group cols
    group_no_meta = [e for e in string_cols if e not in ('MetaSources')]

    # combine/sum columns that share the same data other than Metasources, combining MetaSources string in process
    df_sub = df_sub.groupby(group_no_meta)['MetaSources'].apply(', '.join).reset_index()
    # drop the MetaSources col in original df and replace with the MetaSources col in df_sub
    df = df.drop('MetaSources', 1)
    harmonized_df = df.merge(df_sub, how='left')
    harmonized_df = replace_strings_with_NoneType(harmonized_df)

    return harmonized_df
