# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
import numpy as np
import pandas as pd
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system


def CoA_Cropland_NAICS_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from flowbyactivity.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        # at national level, remove the text string calling for state acronyms
        if x == 'NATIONAL':
            url = build_url
            url = url.replace("__aggLevel__", x)
            url = url.replace("&state_alpha=__stateAlpha__", "")
            url = url.replace(" ", "%20")
            urls.append(url)
        else:
            # substitute in state acronyms for state and county url calls
            for z in state_abbrevs:
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__stateAlpha__", z)
                url = url.replace(" ", "%20")
                urls.append(url)
    return urls


def coa_cropland_NAICS_call(url, coa_response, args):
    cropland_json = json.loads(coa_response.text)
    df_cropland = pd.DataFrame(data=cropland_json["data"])
    return df_cropland


def coa_cropland_NAICS_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria and only includes data on harvested acreage
    (irrigated and total). """
    df = pd.concat(dataframe_list, sort=False)
    # specify desired data based on domain_desc
    df = df[df['domain_desc'] == 'NAICS CLASSIFICATION']
    # only want ag land and farm operations
    df = df[df['short_desc'].str.contains("AG LAND|FARM OPERATIONS")]
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips
    # NAICS classification data
    # flowname
    df.loc[:, 'FlowName'] = df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + df['prodn_practice_desc']
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", ALL PRODUCTION PRACTICES", "", regex=True)
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", ALL CLASSES", "", regex=True)
    # activity consumed/produced by
    df.loc[:, 'Activity'] = df['domaincat_desc']
    df.loc[:, 'Activity'] = df['Activity'].str.replace("NAICS CLASSIFICATION: ", "", regex=True)
    df.loc[:, 'Activity'] = df['Activity'].str.replace('[()]+', '')
    df['ActivityProducedBy'] = np.where(df["unit_desc"] == 'OPERATIONS', df["Activity"], '')
    df['ActivityConsumedBy'] = np.where(df["unit_desc"] == 'ACRES', df["Activity"], '')

    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount", "unit_desc": "Unit",
                            "year": "Year", "CV (%)": "Spread",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['Activity', 'class_desc', 'commodity_desc', 'domain_desc', 'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc', 'domaincat_desc', 'util_practice_desc'])
    # modify contents of units column
    df.loc[df['Unit'] == 'OPERATIONS', 'Unit'] = 'p'
    # modify contents of flowamount column, "D" is supressed data, "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # USDA CoA 2017 states that (H) means CV >= 99.95, therefore replacing with 99.95 so can convert column to int
    # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None  # for instances where data is missing
    df.loc[df['Spread'] == "(D)", 'Spread'] = withdrawn_keyword
    # drop Descriptions that contain certain phrases, as these data are included in other categories
    df = df[~df['Description'].str.contains('FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP')]
    # drop Descriptions that contain certain phrases - only occur in AG LAND data
    df = df[~df['Description'].str.contains('INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE')].reset_index(drop=True)
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # Add hardcoded data
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland_NAICS"
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = None
    df['DataCollection'] = 2
    return df



