# EIA_CBECS_Land.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system , fba_default_grouping_fields, aggregator

"""
2012 Commercial Buildings Energy Consumption Survey (CBECS)
https://www.eia.gov/consumption/commercial/reports/2012/energyusage/index.php 
Last updated: Monday, August 17, 2020
"""


def eia_cbecs_land_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from flowbyactivity.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []
    # replace "__xlsx_name__" in build_url to create three urls
    for x in config['xlsx']:
        url = build_url
        url = url.replace("__xlsx__", x)
        urls.append(url)

    return urls


def eia_cbecs_land_call(url, cbesc_response, args):

    # Convert response to dataframe
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='data').dropna()
    df_raw_rse = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='rse').dropna()

    if("b5.xlsx" in url):
        # skip rows and remove extra rows at end of dataframe
        df_data = pd.DataFrame(df_raw_data.loc[15:34]).reindex()
        df_rse = pd.DataFrame(df_raw_rse.loc[15:34]).reindex()

        df_data.columns = ["Name", "All buildings", "New England", "Middle Atlantic", "East North Central",
                          "West North Central", "South Atlantic",
                          "East South Central", "West South Central",
                          "Mountain", "Pacific"]
        df_rse.columns = ["Name", "All buildings", "New England", "Middle Atlantic", "East North Central",
                          "West North Central", "South Atlantic",
                          "East South Central", "West South Central",
                          "Mountain", "Pacific"]

        df_rse = df_rse.melt(id_vars=["Name"],
                    var_name="Location",
                    value_name="Spread")
        df_data =df_data.melt(id_vars=["Name"],
                    var_name="Location",
                    value_name="FlowAmount")
    elif("b12.xlsx" in url):
        # skip rows and remove extra rows at end of dataframe
        df_data1 = pd.DataFrame(df_raw_data.loc[4:5]).reindex()
        df_data2 = pd.DataFrame(df_raw_data.loc[46:50]).reindex()
        df_data = pd.concat([df_data1, df_data2])
        df_rse1 = pd.DataFrame(df_raw_rse.loc[4:5]).reindex()
        df_rse2 = pd.DataFrame(df_raw_rse.loc[46:50]).reindex()
        df_rse = pd.concat([df_rse1, df_rse2])

        df_data.columns = ["Description", "All buildings", "Office", "Warehouse and storage", "Service",
                           "Mercantile", "Religious worship",
                           "Education", "Public assembly"]
        df_rse.columns = ["Description", "All buildings", "Office", "Warehouse and storage", "Service",
                           "Mercantile", "Religious worship",
                           "Education", "Public assembly"]
        df_rse = df_rse.melt(id_vars=["Description"],
                    var_name="Name",
                    value_name="Spread")
        df_data =df_data.melt(id_vars=["Description"],
                    var_name="Name",
                    value_name="FlowAmount")
    elif ("b14.xlsx" in url):
        # skip rows and remove extra rows at end of dataframe
        df_data = pd.DataFrame(df_raw_data.loc[27:31]).reindex()
        df_rse = pd.DataFrame(df_raw_rse.loc[27:31]).reindex()

        df_data.columns = ["Description", "All buildings", "Food service", "Food sales", "Lodging",
                           "Health care In-Patient", "Health care Out-Patient",
                           "Public order and safety"]
        df_rse.columns = ["Description", "All buildings", "Food service", "Food sales", "Lodging",
                           "Health care In-Patient", "Health care Out-Patient",
                           "Public order and safety"]
        df_rse = df_rse.melt(id_vars=["Description"],
                             var_name="Name",
                             value_name="Spread")
        df_data = df_data.melt(id_vars=["Description"],
                               var_name="Name",
                               value_name="FlowAmount")

    df = pd.merge(df_rse, df_data)
    return df


def eia_cbecs_land_parse(dataframe_list, args):
    """

    :param dataframe_list:
    :param args:
    :return:
    """

    # concat dataframes
    df_array = []
    for dataframes in dataframe_list:
        # rename column(s)
        dataframes = dataframes.rename(columns={'Name': 'ActivityConsumedBy'})
        if "Location" not in list(dataframes):
            dataframes["Location"] = US_FIPS
            dataframes = assign_fips_location_system(dataframes, args['year'])
            dataframes = dataframes.drop(dataframes[dataframes.Description == "Any elevators"].index)
            dataframes["Description"] = dataframes["Description"].apply(lambda x: x if 'All buildings' in x else x + " floors")
        else:
            dataframes = dataframes.drop(dataframes[dataframes.ActivityConsumedBy == "Before 1920"].index)
            # rename location
            dataframes["Name"] = dataframes["Location"] + ' Division'
            dcodes = get_region_and_division_codes()
            dataframes = dataframes.merge(dcodes[['Division', 'Name', 'LocationSystem']], how='left')
            dataframes["Description"] = "All buildings"
            dataframes['Location'] = dataframes['Division'].replace(float("NaN"), US_FIPS)
            dataframes.loc[dataframes.Location == US_FIPS, "LocationSystem"] = "FIPS_2010"
            dataframes = dataframes.drop(columns=['Division', 'Name'])
        df_array.append(dataframes)
    df = pd.concat(df_array, sort=False, ignore_index=True)

    # trim whitespace and standardize Activity names
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
    df = standardize_eia_cbecs_land_activity_names(df, column_to_standardize='ActivityConsumedBy')

    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = withdrawn_keyword
    df["Class"] = 'Land'
    df["SourceName"] = 'EIA_CBECS_Land'
    df['Year'] = args["year"]
    df['FlowName'] = "Commercial, " + df["ActivityConsumedBy"] + ", Total floorspace"
    df['Compartment'] = 'ground'
    df['Unit'] = "million square feet"
    df['MeasureofSpread'] = "RSE"
    df['FlowType'] = 'ELEMENTARY_FLOW'

    # drop any duplicates that arise from joining multiple excel files
    df = df.drop_duplicates()

    return df


def standardize_eia_cbecs_land_activity_names(df, column_to_standardize):
    """
    Activity names vary across csvs. Standardize
    :param df:
    :return:
    """

    from flowsa.common import clean_str_and_capitalize

    # standardize strings in provided column
    df[column_to_standardize] = df[column_to_standardize].replace({'Public Order/ Safety': 'Public order and safety',
                                                                   'Retail (mall)': 'Enclosed and strip malls',
                                                                   'Inpatient': 'Health care In-Patient',
                                                                   'Outpatient': 'Health care Out-Patient',
                                                                   'Inpatient Health Care': 'Health care In-Patient',
                                                                   'Outpatient Health Care': 'Health care Out-Patient',
                                                                   'Retail (non - mall)': 'Retail (other than mall)',
                                                                   'Retail (non-mall)': 'Retail (other than mall)',
                                                                   'Warehouse/ Storage': 'Warehouse and storage'
                                                                   })

    # modify capitalization
    df[column_to_standardize] = df.apply(lambda x: clean_str_and_capitalize(x[column_to_standardize]), axis=1)

    # exception to capitalization rule is health care
    df[column_to_standardize] = df[column_to_standardize].replace({'Health care in-patient': 'Health care In-Patient',
                                                                   'Health care out-patient': 'Health care Out-Patient'
                                                                   })

    return df


def cbecs_land_fba_cleanup(fba):
    """

    :param fba:
    :return:
    """

    # estimate floor space using number of floors
    fba = calculate_floorspace_based_on_number_of_floors(fba)

    # calculate the land area in addition to building footprint
    fba = calculate_total_facility_land_area(fba)

    return fba


def calculate_floorspace_based_on_number_of_floors(fba):
    """
    Estimate total floorspace for each building type based on data on the number of floors for each building type.
    Assumptions (Taken from Yang's static satellite tables):
    1. When floor range is 4-9, assume 6 stories
    2. When floor range is 10 or more, assume 15 stories
    :param fba:
    :return:
    """

    # disaggregate mercentile to malls and non malls
    fba = disaggregate_eia_cbecs_mercentile(fba)

    # disaggregate other and vacant
    fba = disaggregate_eia_cbecs_vacant_and_other(fba)

    # drop data for 'all buildings'
    fba = fba[fba['Description'] != 'All buildings']
    # add column 'DivisionFactor' based on description
    fba = fba.assign(DivisionFactor=fba['Description'].apply(lambda x: (1 if 'One' in x
                                                                        else (2 if 'Two' in x
                                                                              else (3 if 'Three' in x
                                                                                    else (6 if 'Four' in x
                                                                                          else (15 if 'Ten' in x
                                                                                                else ""
                                                                                                )))))))
    # modify flowamounts to represent building footprint rather than total floorspace
    fba['FlowAmount'] = fba['FlowAmount'] / fba['DivisionFactor']
    # sum values for single flowamount for each bulding type
    groupbycols = fba_default_grouping_fields
    fba2 = aggregator(fba, groupbycols)

    # add description
    fba2 = fba2.assign(Description='Building Footprint')

    return fba2


def disaggregate_eia_cbecs_mercentile(df_load):
    """
    Determine the number of floors for malls and non malls based on mercentile data
    :param df_load:
    :return:
    """

    # subset mercantile df into all buildings and number of floors
    df_merc = df_load[df_load['ActivityConsumedBy'].isin(['Mercantile'])].reset_index(drop=True)
    df_merc = df_merc[['FlowAmount', 'Unit', 'Location', 'LocationSystem', 'Year', 'Description']]
    df_merc = df_merc.rename(columns={'FlowAmount': 'Mercantile'})

    df_merc_all = df_merc[df_merc['Description'] == 'All buildings']
    df_merc_all = df_merc_all.drop(columns='Description')
    df_merc_all = df_merc_all.rename(columns={'FlowAmount': 'Mercantile'})

    df_merc_floors = df_merc[df_merc['Description'].str.contains('floors')]
    df_merc_floors = df_merc_floors.rename(columns={'FlowAmount': 'Mercantile'})

    # subset to mall activities and calc ratio of mercantile
    df_mall = df_load[df_load['ActivityConsumedBy'].isin(['Enclosed and strip malls'])].reset_index(drop=True)
    df_mall2 = df_mall.merge(df_merc_all)
    df_mall2['FlowAmount'] = df_mall2['FlowAmount']/df_mall2['Mercantile']
    # drop description col and merge with mercantile floors, recalc flow amount
    df_mall2 = df_mall2.drop(columns=['Description', 'Mercantile'])
    df_mall3 = df_mall2.merge(df_merc_floors)
    df_mall3['FlowAmount'] = df_mall3['FlowAmount'] * df_mall3['Mercantile']
    df_mall3 = df_mall3.drop(columns='Mercantile')

    # repeat with non mall categories
    df_nonmall = df_load[df_load['ActivityConsumedBy'].isin(['Retail (other than mall)'])].reset_index(drop=True)
    df_nonmall2 = df_nonmall.merge(df_merc_all)
    df_nonmall2['FlowAmount'] = df_nonmall2['FlowAmount']/df_nonmall2['Mercantile']
    # drop description col and merge with mercantile floors, recalc flow amount
    df_nonmall2 = df_nonmall2.drop(columns=['Description', 'Mercantile'])
    df_nonmall3 = df_nonmall2.merge(df_merc_floors)
    df_nonmall3['FlowAmount'] = df_nonmall3['FlowAmount'] * df_nonmall3['Mercantile']
    df_nonmall3 = df_nonmall3.drop(columns='Mercantile')

    # concat dfs
    df = pd.concat([df_load, df_mall3, df_nonmall3], ignore_index=True, sort=False)

    # drop mercantile to prevent double counting
    df = df[df['ActivityConsumedBy'] != 'Mercantile']

    return df


def disaggregate_eia_cbecs_vacant_and_other(df_load):
    """
    Determine the number of floors for malls and non malls based on mercentile data
    :param df_load:
    :return:
    """

    # subset df into all buildings and number of floors
    df_all = df_load[df_load['ActivityConsumedBy'].isin(['All buildings'])].reset_index(drop=True)
    df_all = df_all[df_all['Description'] != 'All buildings'].reset_index(drop=True)
    df_all = df_all[['FlowAmount', 'Unit', 'Location', 'LocationSystem', 'Year', 'Description']]
    df_all = df_all.rename(columns={'FlowAmount': 'Total'})

    # all other rows that have information on floors, sum
    df_nvno = df_load[df_load['Description'].str.contains('floors')]
    df_nvno = df_nvno[df_nvno['ActivityConsumedBy'] != 'All buildings']
    df_nvno = df_nvno.groupby(['Unit', 'Location', 'LocationSystem', 'Year',
                               'Description'], as_index=False)[['FlowAmount']].agg("sum")
    df_act = df_nvno.rename(columns={'FlowAmount': 'NonVacantNonOther'})

    # merge df and subtract to determine FlowAmount to allocate to vacant and other activities
    df_rem = df_all.merge(df_act)
    df_rem['Remainder'] = df_rem['Total'] - df_rem['NonVacantNonOther']

    # create ratio of vacant and other
    df_vo = df_load[df_load['ActivityConsumedBy'].isin(['Vacant', 'Other'])].reset_index(drop=True)
    df_vo = df_vo.assign(Denominator=df_vo.groupby(['Location', 'LocationSystem', 'Year'])['FlowAmount'].transform('sum'))
    df_vo['FlowAmount'] = df_vo['FlowAmount'] / df_vo['Denominator']

    # drop description col, merge with info on floors and use the ratios to calculate floor area
    df_vo = df_vo.drop(columns=['Description', 'Denominator'])
    df_vo2 = df_vo.merge(df_rem[['Unit', 'Location', 'LocationSystem', 'Remainder', 'Description']])
    df_vo2['FlowAmount'] = df_vo2['FlowAmount'] * df_vo2['Remainder']
    df_vo2 = df_vo2.drop(columns='Remainder')

    # concat with original df
    df = pd.concat([df_load, df_vo2], ignore_index=True, sort=False)

    return df


def calculate_total_facility_land_area(df):
    """
    In land use calculations, in addition to the provided floor area of buildings, estimate other related land area
    associated with commercial facilities (parking, signage, and landscaped area)
    :param df:
    :return:
    """

    from flowsa.values_from_literature import get_commercial_and_manufacturing_floorspace_to_land_area_ratio

    floor_space_to_land_area_ratio = get_commercial_and_manufacturing_floorspace_to_land_area_ratio()

    df = df.assign(FlowAmount=(df['FlowAmount']/floor_space_to_land_area_ratio) - df['FlowAmount'])

    return df

