# EIA_CBECS_Land.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system

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
      #  elif:

    return urls


def eia_cbecs_land_call(url, cbesc_response, args):
    # Convert response to dataframe
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='data').dropna()
    df_raw_rse = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='rse').dropna()

    if("b5.xlsx" in url):
        # skip rows and remove extra rows at end of dataframe
        df_data = pd.DataFrame(df_raw_data.loc[17:34]).reindex()
        df_rse = pd.DataFrame(df_raw_rse.loc[17:34]).reindex()

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
        df_data = pd.DataFrame(df_raw_data.loc[46:50]).reindex()
        df_rse = pd.DataFrame(df_raw_rse.loc[46:50]).reindex()

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

    # concat dataframes
    df_array = []
    for dataframes in dataframe_list:
        # rename column(s)
        dataframes = dataframes.rename(columns={'Name': 'ActivityConsumedBy'})
        if "Location" not in list(dataframes):
            dataframes["Location"] = "00000"
            dataframes["LocationSystem"] = "FIPS_2010"
            dataframes = dataframes.drop(dataframes[dataframes.Description == "Any elevators"].index)
            dataframes["Description"] = dataframes["Description"] + " floors"
        else:
            dataframes = dataframes.drop(dataframes[dataframes.ActivityConsumedBy == "Before 1920"].index)
            dataframes["Location"] = get_region_and_division_codes()["Division"]
            dataframes['LocationSystem'] = "Census_Division"
            dataframes["Description"] = "All Buildings"
            dataframes['Location'] = dataframes['Location'].replace(float("NaN"), '00000')

            dataframes.loc[dataframes.Location == "00000", "LocationSystem"] = "FIPS_2010"

        df_array.append(dataframes)
    df = pd.concat(df_array, sort=False)

    # standardize Activity names
    df = standardize_eia_cbecs_land_activity_names(df, 'ActivityConsumedBy')

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
                                                                   'Outpatient': 'Health care In-Patient',
                                                                   'Inpatient Health Care': 'Health care In-Patient',
                                                                   'Outpatient Health Care': 'Health care In-Patient',
                                                                   'Retail (non - mall)': 'Retail (other than mall)',
                                                                   'Warehouse/ Storage': 'Warehouse and storage'
                                                                   })

    # first modify capitalization
    df[column_to_standardize] = df.apply(lambda x: clean_str_and_capitalize(x[column_to_standardize]), axis=1)

    return df


def cbecs_land_fba_cleanup(fba):

    # want 'All Buildings'
    # todo: create additional fxn to modify land area based on number of floors in the buildings
    fba = fba[fba['Description'] == 'All Buildings']

    # calculate the land area in addition to building footprint
    fba = calculate_total_facility_land_area(fba)

    return fba


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

