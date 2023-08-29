# USDA_ACUP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions to import and parse USDA Chemical Use Survey
"""
import json
import pandas as pd
from flowsa.location import US_FIPS, abbrev_us_state
from flowsa.common import WITHDRAWN_KEYWORD, log
from flowsa.flowbyfunctions import assign_fips_location_system


def acup_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    url = build_url
    url = url.replace(" ", "%20")
    urls.append(url)

    return urls


def acup_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    response_json = json.loads(resp.text)
    df = pd.DataFrame(data=response_json["data"])

    return df


def acup_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, ignore_index=True)

    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha',
                          'sector_desc', 'country_code', 'begin_code',
                          'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc',
                          'congr_district_code', 'asd_code', 'week_ending',
                          'freq_desc', 'load_time', 'zip_5', 'watershed_desc',
                          'region_desc', 'state_ansi', 'state_name',
                          'country_name', 'county_ansi', 'end_code',
                          'group_desc'])
    # create FIPS column by combining existing columns
    # add county fips when missing
    df.loc[df['county_code'] == '', 'county_code'] = '000'
    df['Location'] = df['state_fips_code'] + df['county_code']
    # modify national level fips
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS

    # address non-NAICS classification data
    # use info from other columns to determine flow name
    df.loc[:, 'FlowName'] = df['domaincat_desc'].apply(
        lambda x: x[x.find("(")+1:x.find(")")])
    # extract data within parenthesis for activity col
    # drop this column later
    df['ActivityConsumedBy'] = f"{df['commodity_desc']}, " \
                               f"{df['class_desc']}, " \
                               f"{df['util_practice_desc']}"
    # not interested in all data from class_desc
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(
        ", ALL CLASSES", "", regex=True)
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(
        ", ALL UTILIZATION PRACTICES", "", regex=True)

    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "Unit",
                            "year": "Year",
                            "CV (%)": "Spread",
                            "short_desc": "Description"
                            }
                   )
    # drop remaining unused columns
    df = df.drop(columns=['class_desc', 'commodity_desc', 'domain_desc',
                          'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc',
                          'domaincat_desc', 'util_practice_desc'])
    # modify contents of flowamount column, "D" is supressed data,
    # "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "(NA)", 'FlowAmount'] = 0
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # USDA CoA 2017 states that (H) means CV >= 99.95, therefore replacing
    # with 99.95 so can convert column to int (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None
    df.loc[df['Spread'] == "(D)", 'Spread'] = WITHDRAWN_KEYWORD

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # Add hardcoded data
    df['Class'] = "Chemicals"
    df['SourceName'] = source
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
