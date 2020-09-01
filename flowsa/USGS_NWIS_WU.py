# USGS_Water_Use.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.common import *
from flowsa.flowbyfunctions import fba_activity_fields, assign_fips_location_system


def usgs_URL_helper(build_url, config, args):
    """ Used to substitute in components of usgs urls"""
    # initiate url list for usgs data
    urls_usgs = []
    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}
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


def usgs_call(url, usgs_response, args):
    """Remove the metadata at beginning of files (lines start with #)"""
    usgs_data = []
    metadata = []
    with io.StringIO(usgs_response.text) as fp:
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
        # rename national level columns to make it easier to concat and parse data frames
        df_usgs = df_usgs.rename(columns={df_usgs.columns[1]: "Description", df_usgs.columns[2]: "FlowAmount"})
    return df_usgs


def usgs_parse(dataframe_list, args):
    """Parsing the usgs data into flowbyactivity format"""

    for df in dataframe_list:
        # add columns at national and state level that only exist at the county level
        if 'state_cd' not in df:
            df['state_cd'] = '00'
        if 'state_name' not in df:
            df['state_name'] = None
        if 'county_cd' not in df:
            df['county_cd'] = '000'
        if 'county_nm' not in df:
            df['county_nm'] = 'None'
        if 'year' not in df:
            df['year'] = args["year"]
    # concat data frame list based on geography and then parse data
    df = pd.concat(dataframe_list, sort=False)
    df_n = df[df['geo'] == 'national']
    df_sc = df[df['geo'] != 'national']
    # drop columns that are all NAs
    df_n = df_n.dropna(axis=1, how='all')
    df_sc = df_sc.dropna(axis=1, how='all')
    # melt state and county level data frame
    df_sc = pd.melt(df_sc, id_vars=["geo", "state_cd", "state_name", "county_cd", "county_nm", "year"],
                    var_name="Description", value_name="FlowAmount")
    # merge national and state/county dataframes
    df = pd.concat([df_n, df_sc], sort=False)
    # drop rows that don't have a record and strip values that have extra symbols
    df.loc[:, 'FlowAmount'] = df['FlowAmount'].str.strip()
    df.loc[:, "FlowAmount"] = df['FlowAmount'].str.replace("a", "", regex=True)
    df.loc[:, "FlowAmount"] = df['FlowAmount'].str.replace("c", "", regex=True)
    df = df[df['FlowAmount'] != '-']
    df = df[df['FlowAmount'] != '']
    # create fips codes by combining columns
    df.loc[:, 'Location'] = df['state_cd'] + df['county_cd']
    # drop unused columns
    df = df.drop(columns=['county_cd', 'county_nm', 'geo', 'state_cd', 'state_name'])
    # create new columns based on description
    df.loc[:, 'Unit'] = df['Description'].str.rsplit(',').str[-1]
    # create flow name column
    df.loc[:, 'FlowName'] = np.where(df.Description.str.contains("fresh"), "fresh",
                     np.where(df.Description.str.contains("saline"), "saline",
                     np.where(df.Description.str.contains("wastewater"), "wastewater", "total")))
    # create flow name column
    df.loc[:, 'Compartment'] = np.where(df.Description.str.contains("ground"), "ground",
                        np.where(df.Description.str.contains("Ground"), "ground",
                        np.where(df.Description.str.contains("surface"), "surface",
                        np.where(df.Description.str.contains("Surface"), "surface",
                        np.where(df.Description.str.contains("instream water use"), "surface",  # based on usgs def
                        np.where(df.Description.str.contains("consumptive"), "air",
                        np.where(df.Description.str.contains("conveyance"), "water",
                        np.where(df.Description.str.contains("total"), "total", "total"))))))))
    # drop rows of data that are not water use/day. also drop "in" in unit column
    df.loc[:, 'Unit'] = df['Unit'].str.strip()
    df.loc[:, "Unit"] = df['Unit'].str.replace("in ", "", regex=True)
    df.loc[:, "Unit"] = df['Unit'].str.replace("In ", "", regex=True)
    df = df[~df['Unit'].isin(["millions", "gallons/person/day", "thousands", "thousand acres", "gigawatt-hours"])]
    df = df[~df['Unit'].str.contains("number of")]
    df.loc[df['Unit'].isin(['Mgal/', 'Mgal']), 'Unit'] = 'Mgal/d'
    df = df.reset_index(drop=True)
    # assign activities to produced or consumed by, using functions defined below
    activities = df['Description'].apply(activity)
    activities.columns = ['ActivityProducedBy', 'ActivityConsumedBy']
    df = df.join(activities)
    # rename year column
    df = df.rename(columns={"year": "Year"})
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # hardcode column information
    df['Class'] = 'Water'
    df['SourceName'] = 'USGS_NWIS_WU'
    # Assign data quality scores
    df.loc[df['ActivityConsumedBy'].isin(['Public Supply', 'Public supply']), 'DataReliability'] = '2'
    df.loc[df['ActivityConsumedBy'].isin(['Aquaculture', 'Livestock', 'Total Thermoelectric Power',
                                          'Thermoelectric power', 'Thermoelectric Power Once-through cooling',
                                          'Thermoelectric Power Closed-loop cooling',
                                          'Wastewater Treatment']), 'DataReliability'] = '3'
    df.loc[df['ActivityConsumedBy'].isin(['Domestic', 'Self-supplied domestic', 'Industrial', 'Self-supplied industrial',
                                          'Irrigation, Crop', 'Irrigation, Golf Courses', 'Irrigation, Total',
                                          'Irrigation', 'Mining']), 'DataReliability'] = '4'
    df.loc[df['ActivityConsumedBy'].isin(['Total withdrawals', 'Total Groundwater',
                                          'Total Surface water']), 'DataReliability'] = '5'
    df.loc[df['ActivityProducedBy'].isin(['Public Supply']), 'DataReliability'] = '2'
    df.loc[df['ActivityProducedBy'].isin(['Aquaculture', 'Livestock', 'Total Thermoelectric Power',
                                          'Thermoelectric Power Once-through cooling',
                                          'Thermoelectric Power Closed-loop cooling',
                                          'Wastewater Treatment']), 'DataReliability'] = '3'
    df.loc[df['ActivityProducedBy'].isin(['Domestic', 'Industrial', 'Irrigation, Crop', 'Irrigation, Golf Courses',
                                          'Irrigation, Total', 'Mining']), 'DataReliability'] = '4'
    # remove commas from activity names
    df.loc[:, 'ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(", ", " ", regex=True)
    df.loc[:, 'ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(", ", " ", regex=True)

    # add FlowType
    df['FlowType'] = np.where(df["Description"].str.contains('wastewater'), "WASTE_FLOW",
                     np.where(df["Description"].str.contains('self-supplied'), "ELEMENTARY_FLOW",
                     np.where(df["Description"].str.contains('Self-supplied'), "ELEMENTARY_FLOW",
                     np.where(df["Description"].str.contains('conveyance'), "ELEMENTARY_FLOW",
                     np.where(df["Description"].str.contains('consumptive'), "ELEMENTARY_FLOW",
                     np.where(df["Description"].str.contains('deliveries'), "TECHNOSPHERE_FLOW",
                                                                            ""))))))

    # standardize usgs activity names
    df = standardize_usgs_nwis_names(df)

    return df


def activity(name):
    """Create rules to assign activities to produced by or consumed by"""

    name_split = name.split(",")
    if "Irrigation" in name and "gal" not in name_split[1]:
        n = name_split[0] + "," + name_split[1]
    else:
        n = name_split[0]

    if " to " in n:
        activity = n.split(" to ")
        name = split_name(activity[0])
        produced = name[0]
        consumed = capitalize_first_letter(activity[1])
    elif " from " in n:
        if ")" in n:
            open_paren_split = n.split("(")
            capitalized_string = capitalize_first_letter(open_paren_split[0])
            close_paren_split = open_paren_split[1].split(")")
            produced_split = close_paren_split[1].split(" from ")
            produced = capitalize_first_letter(produced_split[1].strip())
            consumed = capitalized_string.strip() + " " + close_paren_split[0].strip()
        else:
            activity = n.split(" from ")
            name = split_name(activity[0])
            produced = capitalize_first_letter(activity[1])
            consumed = name[0].strip()
    elif "consumptive" in n:
        if ")" in n:
            open_paren_split = n.split("(")
            capitalized_string = capitalize_first_letter(open_paren_split[0])
            close_paren_split = open_paren_split[1].split(")")
            produced = capitalized_string.strip() + " " + close_paren_split[0].strip()
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
        consumed = capitalized_string.strip() + " " + close_paren_split[0].strip()
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
    """This method splits the header name into a source name and a flow name"""
    space_split = name.split(" ")
    upper_case = ""
    lower_case = ""
    for s in space_split:
        first_letter = s[0]
        if first_letter.isupper():
            upper_case = upper_case.strip() + " " + s
        else:
            lower_case = lower_case.strip() + " " + s
    return (upper_case, lower_case)


def standardize_usgs_nwis_names(flowbyactivity_df):
    """
    The activity names differ at the national level. Method to standardize names to allow for comparison of aggregation
    to national level.

    Used to check geoscale aggregation
    """

    # modify national level compartment
    flowbyactivity_df['Compartment'].loc[
        (flowbyactivity_df['Location'] == '00000') & (flowbyactivity_df['ActivityConsumedBy'] == 'Livestock')] = 'total'
    flowbyactivity_df['FlowName'].loc[
        (flowbyactivity_df['Location'] == '00000') & (flowbyactivity_df['ActivityConsumedBy'] == 'Livestock')] = 'fresh'
    flowbyactivity_df['Compartment'].loc[
        (flowbyactivity_df['Compartment'] is None) & (flowbyactivity_df['Location'] == '00000')] = 'total'

    # standardize activity names across geoscales
    for f in fba_activity_fields:

        flowbyactivity_df[f].loc[flowbyactivity_df[f] == 'Public'] = 'Public Supply'
        flowbyactivity_df[f].loc[flowbyactivity_df[f] == 'Irrigation Total'] = 'Irrigation'
        flowbyactivity_df[f].loc[flowbyactivity_df[f] == 'Total Thermoelectric Power'] = 'Thermoelectric Power'
        flowbyactivity_df[f].loc[flowbyactivity_df[f] == 'Thermoelectric'] = 'Thermoelectric Power'
        flowbyactivity_df[f] = flowbyactivity_df[f].astype(str)

    return flowbyactivity_df


def usgs_fba_data_cleanup(df):
    """
    Clean up the dataframe to prepare for flowbysector. Used in flowbysector.py
    :param df:
    :return:
    """

    from flowsa.common import US_FIPS

    # drop duplicate info of "Public Supply deliveries to"
    df = df.loc[~df['Description'].str.contains("deliveries from public supply")].reset_index(drop=True)

    # drop rows related to wastewater
    # df = df.loc[df['FlowName'] != 'wastewater'].reset_index(drop=True)

    # drop rows of commercial data (because only exists for 3 states), causes issues because linked with public supply
    df = df[~df['Description'].str.lower().str.contains('commercial')]

    # drop flowname = 'total' rows when necessary to prevent double counting
    # subset data where flowname = total and where it does not
    df1 = df.loc[df['FlowName'] == 'total']
    # set conditions for data to keep when flowname = 'total
    c1 = df1['Location'] == US_FIPS
    c2 = (df1['ActivityProducedBy'] is not None) & (df1['ActivityConsumedBy'] is not None)
    # subset data
    df1 = df1.loc[c1 | c2].reset_index(drop=True)

    df2 = df.loc[df['FlowName'] != 'total']

    # concat the two df
    df = pd.concat([df1, df2], sort=False)
    # sort df
    df = df.sort_values(['Location', 'ActivityProducedBy', 'ActivityConsumedBy']).reset_index(drop=True)

    return df


def usgs_fba_w_sectors_data_cleanup(df_wsec, attr):
    """
    Call on functions to modify the fba with sectors df before being allocated to sectors
    Used in flowbysector.py

    :param df_wsec: a dataframe with sectors
    :param attr: activity set attributes
    :return:
    """

    df = modify_sector_length(df_wsec, attr)
    df = filter_out_activities(df, attr)

    return df


def modify_sector_length(df_wsec, attr):
    """
    After assigning sectors to activities, modify the sector length of an activity, to match the assigned sector in
    another sector column (SectorConsumedBy/SectorProducedBy). This is helpful for sector aggregation. The USGS NWIS WU
    "Public Supply" should be modified to match sector length.

    :param df_wsec: a df that includes columns for SectorProducedBy and SectorConsumedBy
    :return:
    """

    # the activity(ies) whose sector length should be modified
    activities = ["Public Supply"]

    # subset data
    df1 = df_wsec.loc[(df_wsec['SectorProducedBy'].isnull()) |
                      (df_wsec['SectorConsumedBy'].isnull())].reset_index(drop=True)
    df2 = df_wsec.loc[(~df_wsec['SectorProducedBy'].isnull()) &
                      (~df_wsec['SectorConsumedBy'].isnull())].reset_index(drop=True)

    # concat data into single dataframe
    if len(df2) != 0:
        df2.loc[:, 'LengthToModify'] = np.where(df2['ActivityProducedBy'].isin(activities),
                                                df2['SectorProducedBy'].str.len(), 0)
        df2.loc[:, 'LengthToModify'] = np.where(df2['ActivityConsumedBy'].isin(activities),
                                                df2['SectorConsumedBy'].str.len(), df2['LengthToModify'])
        df2.loc[:, 'TargetLength'] = np.where(df2['ActivityProducedBy'].isin(activities),
                                              df2['SectorConsumedBy'].str.len(), 0)
        df2.loc[:, 'TargetLength'] = np.where(df2['ActivityConsumedBy'].isin(activities),
                                              df2['SectorProducedBy'].str.len(), df2['TargetLength'])

        df2.loc[:, 'SectorProducedBy'] = df2.apply(
            lambda x: x['SectorProducedBy'][:x['TargetLength']] if x['LengthToModify'] > x['TargetLength']
            else x['SectorProducedBy'], axis=1)
        df2.loc[:, 'SectorConsumedBy'] = df2.apply(
            lambda x: x['SectorConsumedBy'][:x['TargetLength']] if x['LengthToModify'] > x['TargetLength']
            else x['SectorConsumedBy'], axis=1)

        df2.loc[:, 'SectorProducedBy'] = df2.apply(
            lambda x: x['SectorProducedBy'].ljust(x['TargetLength'], '0') if x['LengthToModify'] < x['TargetLength']
            else x['SectorProducedBy'], axis=1)
        df2.loc[:, 'SectorConsumedBy'] = df2.apply(
            lambda x: x['SectorConsumedBy'].ljust(x['TargetLength'], '0') if x['LengthToModify'] < x['TargetLength']
            else x['SectorConsumedBy'], axis=1)

        df2 = df2.drop(columns=["LengthToModify", 'TargetLength'])

        df = pd.concat([df1, df2], sort=False)
    else:
        df = df1.copy()

    return df


def filter_out_activities(df, attr):
    """
    To avoid double counting and ensure that the deliveries from public supplies to another activity are accurately
    accounted for, in some activity sets, need to drop certain rows of data. if direct allocation, drop rows of data
    where an activity in either activity column is not also directly allocated. These non-direct activities are
    captured in other activity allocations
    :param df: a dataframe that has activity consumed/produced by columns
    :param attr: FBS method file activity set attributes
    :return:
    """

    if attr['allocation_method'] == 'direct':
        df = df.loc[(df[fba_activity_fields[0]] != 'Industrial') |
                    (df[fba_activity_fields[1]] != 'Industrial')].reset_index(drop=True)

    return df


# def missing_row_summation(df):
#     """
#     In the event there is missing data for a particular FlowName/Compartment combo, sum together existing data.
#     Summation should occur at lowest geoscale.
#     :param df:
#     :return:
#     """
#
#     from flowsa.flowbyfunctions import create_geoscale_list
#     from flowsa.flowbyfunctions import aggregator, fba_default_grouping_fields
#
#     # testing
#     # df = flow_subset.copy()
#
#     # want rows where compartment is total
#     df = df.loc[df['Compartment'] == 'total'].reset_index(drop=True)
#     # drop wastewater rows
#     df = df.loc[df['FlowName'] != 'wastewater']
#     # create list of activity produced/consumed by pairs
#     activity_pairs = pd.DataFrame([])
#     for a, b in zip(df['ActivityProducedBy'], df['ActivityConsumedBy']):
#         pairs = [a, b]
#         activity_pairs = activity_pairs.append(pd.DataFrame([pairs], columns=['ActivityProducedBy', 'ActivityConsumedby']))
#     activity_pairs = activity_pairs.drop_duplicates().values.tolist()
#
#     # list of us counties in df
#     county_fips = create_geoscale_list(df, 'county')
#     state_fips = create_geoscale_list(df, 'state')
#     us_fips = create_geoscale_list(df, 'national')
#
#     geo_level = (county_fips, state_fips, us_fips)
#
#     for (a, b) in activity_pairs:
#         for i in geo_level:
#             # subset the data based on activity columns
#             df2 = df.loc[(df['ActivityProducedBy'] == a) & (df['ActivityConsumedBy'] == b) &
#                          (df['Location'].isin(i))].reset_index(drop=True)
#             # list of counties that have total/total data
#             df_subset = df2.loc[(df2['FlowName'] == 'total') & (df2['Compartment'] == 'total')]
#             existing_fips = df_subset['Location'][df_subset['Location'].isin(i)].tolist()
#             # drop rows in df that are in the existing counties list
#             df2 = df2.loc[~df2['Location'].isin(existing_fips)].reset_index(drop=True)
#             if len(df2) != 0:
#                 # drop flowname from aggregation
#                 df2 = df2.drop('FlowName', 1)
#                 # aggregate data (weight DQ)
#                 groupcols = fba_default_grouping_fields
#                 groupcols = [e for e in groupcols if e not in 'FlowName']
#                 df3 = aggregator(df2, groupcols)
#                 # set flowname = total
#                 df3['FlowName'] = 'total'
#                 # append new rows to df
#                 df = df.append(df3)
#
#     return df
