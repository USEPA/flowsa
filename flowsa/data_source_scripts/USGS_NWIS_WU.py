# USGS_NWIS_WU.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Helper functions for importing, parsing, formatting USGS Water Use data
"""

import io
import pandas as pd
import numpy as np
from flowsa.location import abbrev_us_state, US_FIPS
from flowsa.common import fba_activity_fields, capitalize_first_letter
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
from flowsa.flowsa_log import vlog
from flowsa.validation import compare_df_units, \
    calculate_flowamount_diff_between_dfs
from flowsa.flowbyactivity import FlowByActivity


def usgs_URL_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    # initiate url list for usgs data
    urls_usgs = []
    # call on state acronyms from common.py
    state_abbrevs = abbrev_us_state
    # replace "__aggLevel__" in build_url to create three urls
    for c in config['agg_levels']:
        # at national level, remove most of the url
        if c == 'National':
            url = build_url
            url = url.replace("__stateAlpha__/", "")
            url = url.replace("&wu_area=__aggLevel__", "")
            url = url.replace("&wu_county=ALL", "")
            urls_usgs.append(url)
        else:
            # substitute in state acronyms for state and county url calls
            for d in state_abbrevs:
                url = build_url
                url = url.replace("__stateAlpha__", d)
                url = url.replace("__aggLevel__", c)
                urls_usgs.append(url)
    return urls_usgs


def usgs_call(*, resp, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    usgs_data = []
    metadata = []
    with io.StringIO(resp.text) as fp:
        for line in fp:
            if line[0] != '#':
                if "16s" not in line:
                    usgs_data.append(line)
            else:
                metadata.append(line)
    # convert response to dataframe
    df_init = pd.DataFrame(data=usgs_data)
    # split line data into columns by tab separation
    df_init = df_init[0].str.split('\t').apply(pd.Series)
    # make first row column names
    df_usgs = pd.DataFrame(df_init.values[1:], columns=df_init.iloc[0])
    # add column denoting geography, used to help parse data
    if "County" in url:
        df_usgs.insert(0, "geo", "county")
    elif "State+Total" in url:
        df_usgs.insert(0, "geo", "state")
    else:
        df_usgs.insert(0, "geo", "national")
        # rename national level columns to make it easier to concat and
        # parse data frames
        df_usgs = df_usgs.rename(columns={df_usgs.columns[1]: "Description",
                                          df_usgs.columns[2]: "FlowAmount"})
    return df_usgs


def usgs_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    for df in df_list:
        # add columns at national and state level that
        # only exist at the county level
        if 'state_cd' not in df:
            df['state_cd'] = '00'
        if 'state_name' not in df:
            df['state_name'] = ''
        if 'county_cd' not in df:
            df['county_cd'] = '000'
        if 'county_nm' not in df:
            df['county_nm'] = ''
        if 'year' not in df:
            df['year'] = year
    # concat data frame list based on geography and then parse data
    df = pd.concat(df_list, sort=False)
    df_n = df[df['geo'] == 'national']
    df_sc = df[df['geo'] != 'national']
    # drop columns that are all NAs
    df_n = df_n.dropna(axis=1, how='all')
    df_sc = df_sc.dropna(axis=1, how='all')
    # melt state and county level data frame
    df_sc = pd.melt(df_sc, id_vars=["geo", "state_cd", "state_name",
                                    "county_cd", "county_nm", "year"],
                    var_name="Description", value_name="FlowAmount")
    # merge national and state/county dataframes
    df = pd.concat([df_n, df_sc], sort=False)
    # drop rows that don't have a record and strip values
    # that have extra symbols
    df.loc[:, 'FlowAmount'] = df['FlowAmount'].str.strip()
    df.loc[:, "FlowAmount"] = df['FlowAmount'].str.replace("a", "", regex=True)
    df.loc[:, "FlowAmount"] = df['FlowAmount'].str.replace("c", "", regex=True)
    df = df[df['FlowAmount'] != '-']
    df = df[df['FlowAmount'] != '']
    # create fips codes by combining columns
    df.loc[:, 'Location'] = df['state_cd'] + df['county_cd']
    # drop unused columns
    df = df.drop(columns=['county_cd', 'county_nm', 'geo',
                          'state_cd', 'state_name'])
    # create new columns based on description
    df.loc[:, 'Unit'] = df['Description'].str.rsplit(',').str[-1]
    # create flow name column
    df.loc[:, 'FlowName'] = np.where(
        df.Description.str.contains("fresh"), "fresh",
        np.where(df.Description.str.contains("saline"), "saline",
                 np.where(df.Description.str.contains("wastewater"),
                          "wastewater", "total")))
    # drop data that is only published by some states/not estimated nationally
    # and that would produce incomplete results
    # instream water use is not withdrawn or consumed
    df = df[~df['Description'].str.contains('instream water use|conveyance')]

    # create flow name column
    df.loc[:, 'Compartment'] = np.where(df.Description.str.contains(
        "ground|Ground"), "ground", "total")
    df.loc[:, 'Compartment'] = np.where(df.Description.str.contains(
        "surface|Surface"), "surface", df['Compartment'])
    # consumptive water use is reported nationally for thermoelectric
    # and irrigation beginning in 2015
    df.loc[:, 'Compartment'] = np.where(df.Description.str.contains(
        "consumptive"), "air", df['Compartment'])

    # drop rows of data that are not water use/day
    # Also drop "in" in unit column
    df.loc[:, 'Unit'] = df['Unit'].str.strip()
    df.loc[:, "Unit"] = df['Unit'].str.replace("in ", "", regex=True)
    df.loc[:, "Unit"] = df['Unit'].str.replace("In ", "", regex=True)
    df = df[~df['Unit'].isin(
        ["millions", "gallons/person/day", "thousands", "thousand acres",
         "gigawatt-hours"])]
    df = df[~df['Unit'].str.contains("number of")]
    df.loc[df['Unit'].isin(['Mgal/', 'Mgal']), 'Unit'] = 'Mgal/d'
    df = df.reset_index(drop=True)
    # assign activities to produced or consumed by, using
    # functions defined below
    activities = df['Description'].apply(activity)
    activities.columns = ['ActivityProducedBy', 'ActivityConsumedBy']
    df = df.join(activities)
    # rename year column
    df = df.rename(columns={"year": "Year"})
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hardcode column information
    df['Class'] = 'Water'
    df['SourceName'] = 'USGS_NWIS_WU'
    # Assign data quality scores
    df.loc[df['ActivityConsumedBy'].isin(
        ['Public Supply', 'Public supply']), 'DataReliability'] = 2
    df.loc[df['ActivityConsumedBy'].isin(
        ['Aquaculture', 'Livestock', 'Total Thermoelectric Power',
         'Thermoelectric power', 'Thermoelectric Power Once-through cooling',
         'Thermoelectric Power Closed-loop cooling', 'Wastewater Treatment']),
           'DataReliability'] = 3
    df.loc[
        df['ActivityConsumedBy'].isin(
            ['Domestic', 'Self-supplied domestic', 'Industrial',
             'Self-supplied industrial', 'Irrigation, Crop',
             'Irrigation, Golf Courses', 'Irrigation, Total', 'Irrigation',
             'Mining']), 'DataReliability'] = 4
    df.loc[df['ActivityConsumedBy'].isin(
        ['Total withdrawals', 'Total Groundwater', 'Total Surface water']),
           'DataReliability'] = 5
    df.loc[df['ActivityProducedBy'].isin(
        ['Public Supply']), 'DataReliability'] = 2
    df.loc[df['ActivityProducedBy'].isin(
        ['Aquaculture', 'Livestock', 'Total Thermoelectric Power',
         'Thermoelectric Power Once-through cooling',
         'Thermoelectric Power Closed-loop cooling',
         'Wastewater Treatment']), 'DataReliability'] = 3
    df.loc[df['ActivityProducedBy'].isin(
        ['Domestic', 'Industrial', 'Irrigation, Crop',
         'Irrigation, Golf Courses', 'Irrigation, Total', 'Mining']),
           'DataReliability'] = 4

    df['DataCollection'] = 5  # tmp

    # remove commas from activity names
    df.loc[:, 'ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(
        ", ", " ", regex=True)
    df.loc[:, 'ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(
        ", ", " ", regex=True)

    # add FlowType
    df['FlowType'] = np.where(df["Description"].str.contains('wastewater'),
                              "WASTE_FLOW", "ELEMENTARY_FLOW")
    # is really a "TECHNOSPHERE_FLOW"
    df['FlowType'] = np.where(df["Description"].str.contains(
        'deliveries'), "ELEMENTARY_FLOW", df['FlowType'])

    # standardize usgs activity names
    df = standardize_usgs_nwis_names(df)

    return df


def activity(name):
    """
    Create rules to assign activities to produced by or consumed by
    :param name: str, activities
    :return: pandas series, values for ActivityProducedBy and
        ActivityConsumedBy
    """

    name_split = name.split(",")
    if "Irrigation" in name and "gal" not in name_split[1]:
        n = name_split[0] + "," + name_split[1]
    else:
        n = name_split[0]

    if " to " in n:
        act = n.split(" to ")
        name = split_name(act[0])
        produced = name[0]
        consumed = capitalize_first_letter(act[1])
    elif " from " in n:
        if ")" in n:
            open_paren_split = n.split("(")
            capitalized_string = capitalize_first_letter(open_paren_split[0])
            close_paren_split = open_paren_split[1].split(")")
            produced_split = close_paren_split[1].split(" from ")
            produced = capitalize_first_letter(produced_split[1].strip())
            consumed = capitalized_string.strip() + " " + \
                       close_paren_split[0].strip()
        else:
            act = n.split(" from ")
            name = split_name(act[0])
            produced = capitalize_first_letter(act[1])
            consumed = name[0].strip()
    elif "consumptive" in n:
        if ")" in n:
            open_paren_split = n.split("(")
            capitalized_string = capitalize_first_letter(open_paren_split[0])
            close_paren_split = open_paren_split[1].split(")")
            produced = capitalized_string.strip() + " " + \
                       close_paren_split[0].strip()
            consumed = None
        else:
            split_case = split_name(n)
            consumed = None
            produced = capitalize_first_letter(split_case[0])
    elif ")" in n:
        produced = None
        open_paren_split = n.split("(")
        capitalized_string = capitalize_first_letter(open_paren_split[0])
        close_paren_split = open_paren_split[1].split(")")
        consumed = capitalized_string.strip() + " " + \
                   close_paren_split[0].strip()
    elif "total deliveries" in n:
        split_case = split_name(n)
        consumed = None
        produced = capitalize_first_letter(split_case[0])
    elif "Self-supplied" in n:
        split_case = split_name(n)
        produced = None
        consumed = capitalize_first_letter(split_case[1])
    else:
        split_case = split_name(n)
        produced = None
        consumed = capitalize_first_letter(split_case[0])
    return pd.Series([produced, consumed])


def split_name(name):
    """
    This method splits the header name into a source name and a flow name
    :param name: str, value includes source and flow name
    :return: strings, source name and flow name for a row
    """
    space_split = name.split(" ")
    upper_case = ""
    lower_case = ""
    for s in space_split:
        first_letter = s[0]
        if first_letter.isupper():
            upper_case = upper_case.strip() + " " + s
        else:
            lower_case = lower_case.strip() + " " + s
    return upper_case, lower_case


def standardize_usgs_nwis_names(flowbyactivity_df):
    """
    The activity names differ at the national level. Method to standardize
    names to allow for comparison of aggregation to national level.
    :param flowbyactivity_df: df, FBA format
    :return: df, FBA format with standardized activity names
    """

    # modify national level compartment
    flowbyactivity_df.loc[
        (flowbyactivity_df['Location'] == '00000') &
        (flowbyactivity_df['ActivityConsumedBy'] == 'Livestock'),
        'Compartment'] = 'total'
    flowbyactivity_df.loc[
        (flowbyactivity_df['Location'] == '00000') &
        (flowbyactivity_df['ActivityConsumedBy'] == 'Livestock'),
        'FlowName'] = 'fresh'
    flowbyactivity_df.loc[
        (flowbyactivity_df['Compartment'] is None) &
        (flowbyactivity_df['Location'] == '00000'), 'Compartment'] = 'total'

    # standardize activity names across geoscales
    for f in fba_activity_fields:
        flowbyactivity_df.loc[
            flowbyactivity_df[f] == 'Public', f] = 'Public Supply'
        flowbyactivity_df.loc[
            flowbyactivity_df[f] == 'Irrigation Total', f] = 'Irrigation'
        flowbyactivity_df.loc[flowbyactivity_df[f] ==
                              'Total Thermoelectric Power', f] = \
            'Thermoelectric Power'
        flowbyactivity_df.loc[flowbyactivity_df[f] == 'Thermoelectric', f] = \
            'Thermoelectric Power'
        flowbyactivity_df[f] = flowbyactivity_df[f].astype(str)

    return flowbyactivity_df


def usgs_fba_data_cleanup(fba: FlowByActivity) -> FlowByActivity:
    """
    Clean up the dataframe to prepare for flowbysector. Used in flowbysector.py
    :param fba: df, FBA format
    :return: df, modified FBA
    """
    vlog.info('Converting Bgal/d to Mgal/d')
    fba['FlowAmount'] = np.where(fba['Unit'] == 'Bgal/d',
                 fba['FlowAmount'] * 1000, fba['FlowAmount'])
    fba['Unit'] = np.where(fba['Unit'] == 'Bgal/d', 'Mgal/d', fba['Unit'])

    # calculated NET PUBLIC SUPPLY by subtracting out deliveries to domestic
    vlog.info('Modify the public supply values to generate '
              'NET public supply by subtracting out deliveries to domestic')
    # dfb = calculate_net_public_supply(dfa)
    dfb = calculate_net_public_supply(fba)

    # check that golf + crop = total irrigation, if not,
    # assign all of total irrigation to crop
    vlog.info('If states do not distinguish between golf and crop '
              'irrigation as a subset of total irrigation, assign '
              'all of total irrigation to crop')
    dfc = check_golf_and_crop_irrigation_totals(dfb)

    # national
    df1 = dfc[dfc['Location'] == US_FIPS]

    # drop flowname = 'total' rows when possible to prevent double counting
    # subset data where flowname = total and where it does not
    vlog.info('Drop rows where the FlowName is total to prevent '
              'double counting at the state and county levels. '
              'Retain rows at national level')
    df2 = dfc[dfc['FlowName'] == 'total']
    # set conditions for data to keep when flowname = 'total
    c1 = df2['Location'] != US_FIPS
    c2 = (~df2['ActivityProducedBy'].isnull()) & \
         (~df2['ActivityConsumedBy'].isnull())
    # subset data
    df2 = df2[c1 & c2].reset_index(drop=True)

    # second subset doesn't have total flowname or total compartment
    df3 = dfc[dfc['FlowName'] != 'total']
    df3 = df3[df3['Compartment'] != 'total']
    df3 = df3[df3['Location'] != US_FIPS]

    # concat the two df
    dfd = pd.concat([df1, df2, df3], ignore_index=True, sort=False)

    return dfd


def calculate_net_public_supply(df_load: FlowByActivity):
    """
    USGS Provides info on the quantity of public supply withdrawals that
    are delivered to domestic use. The USGS PS withdrawals are not necessarily
    greater than/equal to the Domestic deliveries because water can be
    withdrawn in one county and delivered in another (water can also cross
    state lines). Therefore, can/do end up with NEGATIVE net public supply
    values (especially at county-level) and PS water is most accurate at
    national level

    Domestic deliveries are subtracted from public supply. An assumption is
    made that PS deliveries to domestic is fresh water. The national level
    data can then be allocated to end users using the BEA Use tables.
    :param df_load: USGS df
    :return: df with net public supply values
    """

    # subset into 2 dfs, one that contains PS data and one that does not
    df1 = df_load[(df_load[fba_activity_fields[0]] == 'Public Supply') |
                  (df_load[fba_activity_fields[1]] == 'Public Supply')]
    df2 = df_load[(df_load[fba_activity_fields[0]] != 'Public Supply') &
                  (df_load[fba_activity_fields[1]] != 'Public Supply')]

    # drop all deliveries to thermo and industrial
    # (not enough states report the data to make usable)
    df1_sub = df1[~df1[fba_activity_fields[1]].isin(
        ['Industrial', 'Thermoelectric Power',
         'Thermoelectric Power Closed-loop cooling',
         'Thermoelectric Power Once-through cooling'])]

    # drop county level values because cannot use county data
    vlog.info('Dropping county level public supply withdrawals '
              'because will end up with negative values due to '
              'instances of water deliveries coming from surrounding '
              'counties')
    df1_sub = df1_sub[df1_sub['Location'].apply(
        lambda x: x[2:6] == '000')].reset_index(drop=True)

    # df of ps delivered and ps withdrawn and us total
    df_d = df1_sub[df1_sub[fba_activity_fields[0]] == 'Public Supply']
    df_w = df1_sub[df1_sub[fba_activity_fields[1]] == 'Public Supply']
    df_us = df1_sub[df1_sub['Location'] == '00000']
    # split consumed further into fresh water (assumption domestic
    # deliveries are freshwater) assumption that water withdrawal taken
    # equally from ground and surface
    df_w1 = df_w[(df_w['FlowName'] == 'fresh') &
                 (df_w['Compartment'] != 'total')]
    df_w2 = df_w[(df_w['FlowName'] == 'fresh') &
                 (df_w['Compartment'] == 'total')]
    # compare units
    compare_df_units(df_w1, df_w2)
    df_wm = pd.merge(df_w1, df_w2[['FlowAmount', 'Location', 'Unit']],
                     how='left', left_on=['Location', 'Unit'],
                     right_on=['Location', 'Unit'])
    df_wm = df_wm.rename(columns={"FlowAmount_x": "FlowAmount",
                                  "FlowAmount_y": "FlowTotal"})
    # compare units
    compare_df_units(df_wm, df_d)
    # merge the deliveries to domestic
    df_w_modified = pd.merge(df_wm, df_d[['FlowAmount', 'Location']],
                             how='left', left_on='Location',
                             right_on='Location')
    df_w_modified = df_w_modified.rename(
        columns={"FlowAmount_x": "FlowAmount",
                 "FlowAmount_y": "DomesticDeliveries"})

    # create flowratio for ground/surface
    df_w_modified.loc[:, 'FlowRatio'] = \
        df_w_modified['FlowAmount'] / df_w_modified['FlowTotal']

    # calculate new, net total public supply withdrawals
    # will end up with negative values due to instances of water
    # deliveries coming form surrounding counties
    df_w_modified.loc[:, 'FlowAmount'] = \
        df_w_modified['FlowAmount'] - (df_w_modified['FlowRatio'] *
                                       df_w_modified['DomesticDeliveries'])

    net_ps = df_w_modified.drop(columns=["FlowTotal", "DomesticDeliveries"])

    # compare units
    compare_df_units(df_d, net_ps)
    # because assuming domestic is all fresh, drop flowable/context
    # and instead use those column data from the net_ps df
    df_d_modified = df_d.drop(columns=['FlowName', 'Compartment'])
    # Also allocate to ground/surface from state ratios
    df_d_modified = pd.merge(df_d_modified,
                             net_ps[['FlowName', 'Compartment',
                                     'Location', 'FlowRatio']],
                             how='left',
                             on='Location')
    # DC has flowratio of 0, so replace (net public supply = 0)
    df_d_modified['FlowRatio'] = np.where(
        (df_d_modified['Location'] == '11000') &
        (df_d_modified['FlowRatio'].isna()),
        0.5, df_d_modified['FlowRatio'])
    df_d_modified.loc[:, 'FlowAmount'] = \
        df_d_modified['FlowAmount'] * df_d_modified['FlowRatio']
    df_d_modified = df_d_modified.drop(columns=["FlowRatio"])

    net_ps = net_ps.drop(columns=["FlowRatio"])

    # concat dfs back (non-public supply, public supply
    # deliveries, net ps withdrawals)
    modified_ps = pd.concat(
        [df2, df_d_modified, net_ps, df_us], ignore_index=True)

    return modified_ps


def check_golf_and_crop_irrigation_totals(df_load: FlowByActivity):
    """
    Check that golf + crop values equal published irrigation totals.
    If not, assign water to crop irrigation.
    :param df_load: df, USGS water use
    :return: df, FBA with reassigned irrigation water to crop and golf
    """
    # drop national data
    df = df_load[df_load['Location'] != '00000']

    df_m2 = subset_and_merge_irrigation_types(df)

    df_m3 = df_m2[df_m2['Diff'] > 0].reset_index(drop=True)

    # rename irrigation to irrigation crop and append rows to df
    df_m3.loc[df_m3['ActivityProducedBy'] ==
              'Irrigation', 'ActivityProducedBy'] = 'Irrigation Crop'
    df_m3.loc[df_m3['ActivityConsumedBy'] ==
              'Irrigation', 'ActivityConsumedBy'] = 'Irrigation Crop'
    df_m3['Description'] = df_m3['Description'].str.replace(
        'Irrigation, Total', 'Irrigation, Crop').str.replace(
        'withdrawals', 'withdrawals for crops').str.replace(
        'use', 'use for crops')
    df_m3 = df_m3.drop(columns=['Golf_Amount', 'Golf_APB', 'Golf_ACB',
                                'Crop_Amount', 'Crop_APB',
                                'Crop_ACB', 'subset_sum', 'FlowAmount',
                                'Crop_Description'])
    df_m3 = df_m3.rename(columns={'Diff': 'FlowAmount'})

    if len(df_m3) != 0:
        df_w_missing_crop = pd.concat([df_load, df_m3], ignore_index=True)

        group_cols = list(df.select_dtypes(include=['object', 'int']).columns)

        df_w_missing_crop2 = df_w_missing_crop.aggregate_flowby(
            retain_zeros=True, columns_to_group_by=group_cols,
            aggregate_ratios=True)

        # validate results - the differences should all be 0
        df_check = subset_and_merge_irrigation_types(df_w_missing_crop2)
        df_check = df_check[df_check['Location'] != US_FIPS].reset_index(
            drop=True)
        df_check['Diff'] = df_check['Diff'].apply(lambda x: round(x, 2))
        df_check2 = df_check[df_check['Diff'] != 0]
        if len(df_check2) > 0:
            vlog.info('The golf and crop irrigation do not add up to '
                      'total irrigation.')
        else:
            vlog.info('The golf and crop irrigation add up to total '
                      'irrigation.')
        return df_w_missing_crop2
    else:
        return df_load


def subset_and_merge_irrigation_types(df: FlowByActivity):
    # subset into golf, crop, and total irrigation (and non irrigation)
    df_i = df[(df[fba_activity_fields[0]] == 'Irrigation') |
              (df[fba_activity_fields[1]] == 'Irrigation')]
    df_g = df[(df[fba_activity_fields[0]] == 'Irrigation Golf Courses') |
              (df[fba_activity_fields[1]] == 'Irrigation Golf Courses')]
    df_c = df[(df[fba_activity_fields[0]] == 'Irrigation Crop') |
              (df[fba_activity_fields[1]] == 'Irrigation Crop')]

    # unit check
    compare_df_units(df_i, df_g)
    # merge the golf and total irrigation into crop df and
    # modify crop FlowAmounts if necessary
    df_m = pd.merge(df_i,
                    df_g[['FlowName', 'FlowAmount', 'ActivityProducedBy',
                          'ActivityConsumedBy', 'Compartment', 'Location',
                          'Year']], how='outer',
                    on=['FlowName', 'Compartment', 'Location', 'Year'])
    df_m = df_m.rename(columns={"FlowAmount_x": "FlowAmount",
                                "ActivityProducedBy_x": "ActivityProducedBy",
                                "ActivityConsumedBy_x": "ActivityConsumedBy",
                                "FlowAmount_y": "Golf_Amount",
                                "ActivityProducedBy_y": "Golf_APB",
                                "ActivityConsumedBy_y": "Golf_ACB",
                                })
    compare_df_units(df_m, df_c)
    df_m2 = pd.merge(df_m,
                     df_c[['FlowName', 'FlowAmount', 'ActivityProducedBy',
                           'ActivityConsumedBy', 'Compartment',
                           'Location', 'Year', 'Description']],
                     how='outer',
                     right_on=['FlowName', 'Compartment', 'Location', 'Year'],
                     left_on=['FlowName', 'Compartment', 'Location', 'Year'])
    df_m2 = df_m2.rename(columns={"FlowAmount_x": "FlowAmount",
                                  "ActivityProducedBy_x": "ActivityProducedBy",
                                  "ActivityConsumedBy_x": "ActivityConsumedBy",
                                  "FlowAmount_y": "Crop_Amount",
                                  "ActivityProducedBy_y": "Crop_APB",
                                  "ActivityConsumedBy_y": "Crop_ACB",
                                  "Description_x": 'Description',
                                  "Description_y": "Crop_Description"})

    # ensure activity cols are object, not float, so not converted to 0
    col = ['ActivityProducedBy', 'Golf_APB', 'Crop_APB']
    for c in col:
        df_m2[c] = df_m2[c].astype(str)


    # fill na and sum crop and golf
    for col in df_m2:
        if df_m2[col].dtype in ("int", "float"):
            df_m2[col] = df_m2[col].fillna(0)
    df_m2['subset_sum'] = df_m2['Crop_Amount'] + df_m2['Golf_Amount']
    df_m2['Diff'] = df_m2['FlowAmount'] - df_m2['subset_sum']

    return df_m2
