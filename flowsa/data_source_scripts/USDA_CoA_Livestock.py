# USDA_CoA_Livestock.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions used to import and parse USDA Census of Ag Livestock data
"""

import json
import pandas as pd
from flowsa.location import US_FIPS, abbrev_us_state
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system


def CoA_Livestock_URL_helper(*, build_url, config, **_):
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
    urls_livestock = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrev = abbrev_us_state
    state_abbrev = {k: v for (k, v) in state_abbrev.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        # at national level, remove the text string calling for state acronyms
        if x == 'NATIONAL':
            url_ls = build_url
            url_ls = url_ls.replace("__aggLevel__", x)
            url_ls = url_ls.replace("&state_alpha=__stateAlpha__", "")
            urls_livestock.append(url_ls)
        else:
            # substitute in state acronyms for state and county url calls
            for y in state_abbrev:
                url_ls = build_url
                url_ls = url_ls.replace("__aggLevel__", x)
                url_ls = url_ls.replace("__stateAlpha__", y)
                urls_livestock.append(url_ls)
    return urls_livestock


def coa_livestock_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    livestock_json = json.loads(resp.text)
    # Convert response to dataframe
    df_livestock = pd.DataFrame(data=livestock_json["data"])
    return df_livestock


def coa_livestock_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # # specify desired data based on domain_desc
    df = df[df['domain_desc'].str.contains("INVENTORY|TOTAL")]
    df = df[~df['domain_desc'].str.contains(
        "ECONOMIC CLASS|NAICS|FARM SALES|AREA OPERATED")]
    # drop any specialized production practices
    df = df[df['prodn_practice_desc'] == 'ALL PRODUCTION PRACTICES']
    # drop specialized class descriptions
    df = df[~df['class_desc'].str.contains("BREEDING|MARKET")]
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha',
                          'sector_desc', 'country_code', 'begin_code',
                          'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc',
                          'congr_district_code', 'asd_code', 'week_ending',
                          'freq_desc', 'load_time', 'zip_5',
                          'watershed_desc', 'region_desc', 'state_ansi',
                          'state_name', 'country_name', 'county_ansi',
                          'end_code', 'group_desc', 'util_practice_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS
    # combine column information to create activity information,
    # and create two new columns for activities
    # drop this column later
    df['ActivityProducedBy'] = df['commodity_desc'] + ', ' + df['class_desc']
    # not interested in all class_desc data
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(
        ", ALL CLASSES", "", regex=True)
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "FlowName",
                            "year": "Year",
                            "CV (%)": "Spread",
                            "domaincat_desc": "Compartment",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['class_desc', 'commodity_desc', 'state_fips_code',
                          'county_code', 'statisticcat_desc',
                          'prodn_practice_desc'])
    # modify contents of flowamount column, "D" is supressed data,
    # "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    # df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # # USDA CoA 2017 states that (H) means CV >= 99.95,
    # therefore replacing with 99.95 so can convert column to int
    # # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None
    df.loc[df['Spread'] == "(D)", 'Spread'] = WITHDRAWN_KEYWORD
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # # Add hardcoded data
    df['Class'] = "Other"
    df['SourceName'] = "USDA_CoA_Livestock"
    df['Unit'] = "p"
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 2
    return df
