# USGS_Water_Use.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.common import *
from flowsa.flowbyfunctions import fba_activity_fields, fbs_activity_fields, assign_fips_location_system


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
            df['state_name'] = ''
        if 'county_cd' not in df:
            df['county_cd'] = '000'
        if 'county_nm' not in df:
            df['county_nm'] = ''
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
                     np.where(df["Description"].str.contains('deliveries'), "ELEMENTARY_FLOW", # is really a "TECHNOSPHERE_FLOW"
                                                              None))))))

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

    # drop rows of commercial data (because only exists for 3 states), causes issues because linked with public supply
    df = df[~df['Description'].str.lower().str.contains('commercial')]

    # calculated NET PUBLIC SUPPLY by subtracting out deliveries to domestic
    df = calculate_net_public_supply(df)

    # check that golf + crop = total irrigation, if not, assign all of total irrigation to crop
    df = check_golf_and_crop_irrigation_totals(df)

    # national
    df1 = df[df['Location'] == US_FIPS]

    # drop flowname = 'total' rows when possible to prevent double counting
    # subset data where flowname = total and where it does not
    df2 = df[df['FlowName'] == 'total']
    # set conditions for data to keep when flowname = 'total
    c1 = df2['Location'] != US_FIPS
    c2 = (~df2['ActivityProducedBy'].isnull()) & (~df2['ActivityConsumedBy'].isnull())
    # subset data
    df2 = df2[c1 & c2].reset_index(drop=True)

    # second subset doesn't have total flowname or total compartment
    df3 = df[df['FlowName'] != 'total']
    df3 = df3[df3['Compartment'] != 'total']
    df3 = df3[df3['Location'] != US_FIPS]

    # concat the two df
    df = pd.concat([df1, df2, df3], ignore_index=True, sort=False)

    return df


def calculate_net_public_supply(df):
    """
    USGS Provides info on the quantity of public supply withdrawals that are delivered to domestic use.
    The USGS PS withdrawals are not necessarily greater than/equal to the Domestic deliveries because water can be
    withdrawn in one county and delivered in another (water can also cross state lines). Therefore, can/do end up
    with NEGATIVE net public supply values and PS water should only be used at a national level

    Domestic deliveries are subtracted from public supply. An assumption is made that PS deliveries to domestic is
    fresh water. The national level data can then be allocated to end users using the BEA Use tables.
    :param df: USGS df
    :return:
    """

    # drop duplicate info of "Public Supply deliveries to"
    df = df.loc[~df['Description'].str.contains("Public Supply total deliveries")]
    df = df.loc[~df['Description'].str.contains("deliveries from public supply")].reset_index(drop=True)

    # subset into 2 dfs, one that contains PS data and one that does not
    df1 = df[(df[fba_activity_fields[0]] == 'Public Supply') |
             (df[fba_activity_fields[1]] == 'Public Supply')]
    df2 = df[(df[fba_activity_fields[0]] != 'Public Supply') &
             (df[fba_activity_fields[1]] != 'Public Supply')]
    # drop all deliveries to thermo and industrial (not enough states report the data to make useable)
    df1_sub = df1[~df1[fba_activity_fields[1]].isin(['Industrial', 'Thermoelectric Power',
                                                     'Thermoelectric Power Closed-loop cooling',
                                                     'Thermoelectric Power Once-through cooling'])]
    # df of ps delivered and ps withdrawan and us total
    df_d = df1_sub[df1_sub[fba_activity_fields[0]] == 'Public Supply']
    df_w = df1_sub[df1_sub[fba_activity_fields[1]] == 'Public Supply']
    df_us = df1_sub[df1_sub['Location'] == '00000']
    # split consumed further into fresh water (assumption domestic deliveries are freshwater)
    # temporary assumption that water withdrawal taken evenly from ground and surface
    df_w1 = df_w[(df_w['FlowName'] == 'fresh') & (df_w['Compartment'] != 'total')]
    df_w2 = df_w[(df_w['FlowName'] == 'fresh') & (df_w['Compartment'] == 'total')]
    df_wm = pd.merge(df_w1, df_w2[['FlowAmount', 'Location']], how='left', left_on='Location', right_on='Location')
    df_wm = df_wm.rename(columns={"FlowAmount_x": "FlowAmount",
                                  "FlowAmount_y": "FlowTotal"
                                  })
    # merge the deliveries to domestic
    df_w_modified = pd.merge(df_wm, df_d[['FlowAmount', 'Location']], how='left', left_on='Location', right_on='Location')
    df_w_modified = df_w_modified.rename(columns={"FlowAmount_x": "FlowAmount",
                                         "FlowAmount_y": "DomesticDeliveries"})

    # create flowratio for ground/surface
    df_w_modified.loc[:, 'FlowRatio'] = df_w_modified['FlowAmount'] / df_w_modified['FlowTotal']
    # calculate new, net total public supply withdrawals
    # will end up with negative values due to instances of water deliveries coming form surrounding counties
    df_w_modified.loc[:, 'FlowAmount'] = df_w_modified['FlowAmount'] - \
                                         (df_w_modified['FlowRatio'] * df_w_modified['DomesticDeliveries'])

    net_ps = df_w_modified.drop(columns=["FlowTotal", "DomesticDeliveries"])

    # because assumiming domestic is all fresh, change flow name. also allocate to ground/surface from state ratios
    df_d_modified = pd.merge(df_d, net_ps[['Compartment', 'Location', 'FlowRatio']], how='left', left_on='Location',
                             right_on='Location')
    df_d_modified.loc[:, 'FlowAmount'] = df_d_modified['FlowAmount'] * df_d_modified['FlowRatio']
    df_d_modified.loc[:, 'FlowName'] = 'fresh'
    df_d_modified = df_d_modified.rename(columns={"Compartment_y": "Compartment"})
    df_d_modified = df_d_modified.drop(columns=["Compartment_x", "FlowRatio"])

    net_ps = df_w_modified.drop(columns=["FlowRatio"])

    # concat dfs back (non-public supply, public supply deliveries, net ps withdrawals)
    modified_ps = pd.concat([df2, df_d_modified, net_ps, df_us], ignore_index=True, sort=True)

    return modified_ps


def check_golf_and_crop_irrigation_totals(df_load):
    """
    Check that golf + crop values equal published irrigation totals. If not, assign water to crop irrigation.

    Resuult : 2010 and 2015 published data correct
    :param df:
    :return:
    """

    # drop national data
    df = df_load[df_load['Location'] != '00000']

    # subset into golf, crop, and total irrigation (and non irrigation)
    df_i = df[(df[fba_activity_fields[0]] == 'Irrigation') |
              (df[fba_activity_fields[1]] == 'Irrigation')]
    df_g = df[(df[fba_activity_fields[0]] == 'Irrigation Golf Courses') |
              (df[fba_activity_fields[1]] == 'Irrigation Golf Courses')]
    df_c = df[(df[fba_activity_fields[0]] == 'Irrigation Crop') |
              (df[fba_activity_fields[1]] == 'Irrigation Crop')]

    # merge the golf and total irrigation into crop df and modify crop FlowAmounts if necessary
    df_m = pd.merge(df_i, df_g[['FlowName', 'FlowAmount', 'ActivityProducedBy', 'ActivityConsumedBy', 'Compartment',
                                'Location', 'Year']],
                    how='outer',
                    right_on=['FlowName', 'Compartment', 'Location', 'Year'],
                    left_on=['FlowName', 'Compartment', 'Location', 'Year'])
    df_m = df_m.rename(columns={"FlowAmount_x": "FlowAmount",
                                "ActivityProducedBy_x": "ActivityProducedBy",
                                "ActivityConsumedBy_x": "ActivityConsumedBy",
                                "FlowAmount_y": "Golf_Amount",
                                "ActivityProducedBy_y": "Golf_APB",
                                "ActivityConsumedBy_y": "Golf_ACB",
                                })
    df_m2 = pd.merge(df_m, df_c[['FlowName', 'FlowAmount', 'ActivityProducedBy', 'ActivityConsumedBy', 'Compartment',
                                 'Location', 'Year']],
                     how='outer',
                     right_on=['FlowName', 'Compartment', 'Location', 'Year'],
                     left_on=['FlowName', 'Compartment', 'Location', 'Year'])
    df_m2 = df_m2.rename(columns={"FlowAmount_x": "FlowAmount",
                                  "ActivityProducedBy_x": "ActivityProducedBy",
                                  "ActivityConsumedBy_x": "ActivityConsumedBy",
                                  "FlowAmount_y": "Crop_Amount",
                                  "ActivityProducedBy_y": "Crop_APB",
                                  "ActivityConsumedBy_y": "Crop_ACB",
                                  })

    # fill na and sum crop and golf
    # df_m2 = df_m2.fillna(0)
    df_m2['subset_sum'] = df_m2['Crop_Amount'] + df_m2['Golf_Amount']
    df_m2['Diff'] = df_m2['FlowAmount'] - df_m2['subset_sum']

    df_m3 = df_m2[df_m2['Diff'] >= 0.000001].reset_index(drop=True)

    # rename irrigation to irrgation crop and append rows to df
    df_m3.loc[df_m3['ActivityProducedBy'] == 'Irrigation', 'ActivityProducedBy'] = 'Irrigation Crop'
    df_m3.loc[df_m3['ActivityConsumedBy'] == 'Irrigation', 'ActivityConsumedBy'] = 'Irrigation Crop'
    df_m3 = df_m3.drop(columns=['Golf_Amount', 'Golf_APB', 'Golf_ACB', 'Crop_Amount', 'Crop_APB',
                                'Crop_ACB', 'subset_sum', 'Diff'])

    if len(df_m3) != 0:
        df_w_missing_crop = df_load.append(df_m3, sort=True, ignore_index=True)
    else:
        df_w_missing_crop = df_load.copy()

    return df_w_missing_crop


def usgs_fba_w_sectors_data_cleanup(df_wsec, attr):
    """
    Call on functions to modify the fba with sectors df before being allocated to sectors
    Used in flowbysector.py
    :param df_wsec: a dataframe with sectors
    :param attr: activity set attributes
    :return:
    """

    df = modify_sector_length(df_wsec)
    df = filter_out_activities(df, attr)
    # df = modify_thermo_and_aqua_sector_assignments(df)

    return df


def modify_sector_length(df_wsec):
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
    df2 = df_wsec.loc[(df_wsec['SectorProducedBy'].notnull()) &
                      (df_wsec['SectorConsumedBy'].notnull())].reset_index(drop=True)

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

        df = pd.concat([df1, df2], sort=True)
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

    # if the activity is public supply and the method is direct, drop rows of industrial and domestic because
    # accounted for in other activity sets
    if (attr['allocation_method'] == 'direct') & ('Public Supply' in attr['names']):
        # drop rows of Industrial
        df = df.loc[(df[fba_activity_fields[0]] != 'Industrial') &
                    (df[fba_activity_fields[1]] != 'Industrial')].reset_index(drop=True)
        # drop rows of Domestic
        df = df.loc[(df[fba_activity_fields[0]] != 'Domestic') &
                    (df[fba_activity_fields[1]] != 'Domestic')].reset_index(drop=True)

    return df


# def modify_thermo_and_aqua_sector_assignments(df):
#     """
#     Currently, do not have a method to allocate water withdrawal beyond four digits for Thermoelectric and Aquaculture.
#     Therefore, after mapping these 2 activities to sectors, drop associated sectors > 4 digits.
#     :param df:
#     :return:
#     """
#
#     from flowsa.flowbyfunctions import replace_NoneType_with_empty_cells, replace_strings_with_NoneType
#
#     activities_to_modify = ("Aquaculture", "Thermoelectric Power")
#     max_sector_length = 4
#
#     # if activities are in the activities to modify length and if sector length is greater than max specified, drop rows
#     # tmp set None to "" so len(x) fxn woks
#     df = replace_NoneType_with_empty_cells(df)
#     # set conditions
#     c1 = df[fba_activity_fields[0]].isin(activities_to_modify)
#     c2 = df[fba_activity_fields[1]].isin(activities_to_modify)
#     c3 = df[fbs_activity_fields[0]].apply(lambda x: len(x) > max_sector_length)
#     c4 = df[fbs_activity_fields[1]].apply(lambda x: len(x) > max_sector_length)
#     # subset data
#     df_modified = df.loc[~((c1 | c2) & (c3 | c4))].reset_index(drop=True)
#     # set '' back to None
#     df_modified = replace_strings_with_NoneType(df_modified)
#
#     return df_modified
