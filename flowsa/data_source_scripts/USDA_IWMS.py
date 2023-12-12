# USDA_IWMS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions used to import and parse USDA Irrigation and
Water Management Survey data
"""

import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS, get_state_FIPS
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units, filter_by_geoscale
from flowsa.flowbyactivity import FlowByActivity


def iwms_url_helper(*, build_url, config, **_):
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
    # initiate url list for coa cropland data
    urls_iwms = []

    # replace "__aggLevel__" in build_url to create two urls
    for x in config['agg_levels']:
        url = build_url
        url = url.replace("__aggLevel__", x)
        urls_iwms.append(url)
    return urls_iwms


def iwms_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    iwms_json = json.loads(resp.text)
    # Convert response to dataframe
    df_iwms = pd.DataFrame(data=iwms_json["data"])
    return df_iwms


def iwms_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False, ignore_index=True)
    # only interested in total water applied, not water
    # applied by type of irrigation
    df = df[df['domain_desc'] == 'TOTAL']
    # drop unused columns
    df = df.drop(
        columns=['CV (%)', 'agg_level_desc', 'location_desc', 'state_alpha',
                 'sector_desc', 'country_code', 'begin_code',
                 'watershed_code', 'reference_period_desc', 'asd_desc',
                 'county_name', 'source_desc', 'congr_district_code',
                 'asd_code', 'week_ending', 'freq_desc', 'load_time',
                 'zip_5', 'watershed_desc', 'region_desc', 'state_ansi',
                 'state_name', 'country_name', 'county_ansi', 'end_code',
                 'group_desc', 'util_practice_desc', 'class_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS
    # create activityconsumedby column
    df['ActivityConsumedBy'] = df['short_desc'].str.split(', IRRIGATED').str[0]
    # not interested in all data from class_desc
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(
        ", IN THE OPEN", "", regex=True)
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
    df = assign_fips_location_system(df, year)
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
    df.loc[df['Compartment'] ==
           'IN THE OPEN, IRRIGATED', 'Compartment'] = 'IRRIGATED'

    return df
