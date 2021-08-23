# USDA_IWMS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions used to import and parse USDA Irrigation and Water Management Survey data
"""

import json
import pandas as pd
from flowsa.common import US_FIPS, WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USDA_CoA_Cropland import disaggregate_pastureland, \
    disaggregate_cropland


def iwms_url_helper(**kwargs):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for data imports that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param kwargs: potential arguments include:
                   build_url: string, base url
                   config: dictionary, items in FBA method yaml
                   args: dictionary, arguments specified when running flowbyactivity.py
                   flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """

    # load the arguments necessary for function
    build_url = kwargs['build_url']
    config = kwargs['config']

    # initiate url list for coa cropland data
    urls_iwms = []

    # replace "__aggLevel__" in build_url to create two urls
    for x in config['agg_levels']:
        url = build_url
        url = url.replace("__aggLevel__", x)
        url = url.replace(" ", "%20")
        urls_iwms.append(url)
    return urls_iwms


def iwms_call(**kwargs):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: potential arguments include:
                   url: string, url
                   response_load: df, response from url call
                   args: dictionary, arguments specified when running
                   flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # load arguments necessary for function
    response_load = kwargs['r']

    iwms_json = json.loads(response_load.text)
    # Convert response to dataframe
    df_iwms = pd.DataFrame(data=iwms_json["data"])
    return df_iwms


def iwms_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    dataframe_list = kwargs['dataframe_list']
    args = kwargs['args']

    df = pd.concat(dataframe_list, sort=False, ignore_index=True)
    # only interested in total water applied, not water applied by type of irrigation
    df = df[df['domain_desc'] == 'TOTAL']
    # drop unused columns
    df = df.drop(columns=['CV (%)', 'agg_level_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code',
                          'asd_code', 'week_ending', 'freq_desc', 'load_time', 'zip_5',
                          'watershed_desc', 'region_desc', 'state_ansi', 'state_name',
                          'country_name', 'county_ansi', 'end_code', 'group_desc',
                          'util_practice_desc', 'class_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips
    # create activityconsumedby column
    df['ActivityConsumedBy'] = df['short_desc'].str.split(', IRRIGATED').str[0]
    # not interested in all data from class_desc
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(", IN THE OPEN", "",
                                                                    regex=True)
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "Unit",
                            "year": "Year",
                            "short_desc": "Description",
                            "prodn_practice_desc": "Compartment",
                            "statisticcat_desc": "FlowName"})
    # drop remaining unused columns
    df = df.drop(columns=['commodity_desc', 'state_fips_code',
                          'county_code', 'domain_desc', 'domaincat_desc'])
    # modify contents of flowamount column, "D" is supressed data,
    # "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # # Add hardcoded data
    df.loc[df['Unit'] == 'ACRES', 'Class'] = 'Land'
    df.loc[df['Unit'] == 'ACRE FEET / ACRE', 'Class'] = 'Water'
    df['SourceName'] = "USDA_IWMS"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # drop rows of unused data
    df = df[~df['ActivityConsumedBy'].str.contains(
        'CUT CHRISTMAS|SOD|FLORICULTURE|UNDER PROTECTION|'
        'HORTICULTURE, OTHER|NURSERY|PROPAGATIVE|LETTUCE')].reset_index(
        drop=True)
    # standardize compartment names for irrigated land
    df.loc[df['Compartment'] == 'IN THE OPEN, IRRIGATED', 'Compartment'] = 'IRRIGATED'

    return df


def disaggregate_iwms_to_6_digit_naics(df, attr, method, **kwargs):
    """
    Disaggregate the data in the USDA Irrigation and Water Management Survey
    to 6-digit NAICS using Census of Agriculture 'Land in Farm' data
    :param df: df, FBA format
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: dictionary, FBS method yaml
    :return: df, FBA format with disaggregated NAICS
    """

    # define sector column to base df modifications
    sector_column = 'SectorConsumedBy'

    # address double counting brought on by iwms categories applying to multiply NAICS
    df.drop_duplicates(subset=['FlowName', 'FlowAmount',
                               'Compartment', 'Location'], keep='first', inplace=True)
    years = [attr['allocation_source_year'] - 1]
    df = df[~df[sector_column].isna()]
    df = disaggregate_pastureland(df, attr, method, years, sector_column)
    df = disaggregate_cropland(df, attr, method, years, sector_column)

    return df
