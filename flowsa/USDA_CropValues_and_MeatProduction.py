# USDA_CropValues_and_MeatProduction.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
import numpy as np
import pandas as pd
from flowsa.common import *


def prodvalue_url_helper(build_url, config, args):
    """This helper function uses the "build_url" input from datapull.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrev = abbrev_us_state
    state_abbrev = {k: v for (k, v) in state_abbrev.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        # for y in config['sector_levels']:
        # at national level, remove the text string calling for state acronyms
        if x == 'NATIONAL':
            url = build_url
            url = url.replace("__aggLevel__", x)
            #url = url.replace("__secLevel__", y)
            url = url.replace("&state_alpha=__stateAlpha__", "")
            url = url.replace(" ", "%20")
            urls.append(url)
        else:
            # substitute in state acronyms for state and county url calls
            for z in state_abbrev:
                url = build_url
                url = url.replace("__aggLevel__", x)
                #url = url.replace("__secLevel__", y)
                url = url.replace("__stateAlpha__", z)
                url = url.replace(" ", "%20")
                urls.append(url)
    return urls


def prodvalue_call(url, prodvalue_response, args):
    prodvalue_json = json.loads(prodvalue_response.text)
    df = pd.DataFrame(data=prodvalue_json["data"])
    return df


def prodvalue_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria and only includes data on harvested acreage
    (irrigated and total). Data is split into two parquets, one for acreage and the other for operations"""
    df = pd.concat(dataframe_list, sort=True)
    # only want data included in select categories
    df = df[~df['group_desc'].isin(['FIELD CROPS', 'FRUIT & TREE NUTS', 'HORTICULTURE', 'VEGETABLES'])]
    df = df[~df['statisticcat_desc'].isin(['DISTRIBUTION', 'FARM USE', 'EXPENSE', 'VALUE', 'SALES',
                                           'SALES FOR SLAUGHTER', 'ASSET VALUE'])]
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'domain_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc', 'CV (%)',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips
    # combine column information to create activity information, and create two new columns for activities
    df['ActivityProducedBy'] = df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + df['util_practice_desc']
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(", ALL CLASSES", "", regex=True)  # not interested in all data from class_desc
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(", ALL UTILIZATION PRACTICES", "", regex=True)  # not interested in all data from class_desc
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "Unit",
                            "year": "Year",
                            'statisticcat_desc': "Compartment",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['class_desc', 'commodity_desc', 'state_fips_code', 'county_code',
                          'prodn_practice_desc', 'domaincat_desc', 'util_practice_desc'])
    # modify contents of flowamount column, "D" is supressed data, "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(S)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(NA)", 'FlowAmount'] = '0'
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # drop Descriptions that contain certain phrases, as these data are included in other categories
    df = df[~df['Description'].str.contains('FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP')]
    # drop Descriptions that contain certain phrases - only occur in AG LAND data
    df = df[~df['Description'].str.contains('INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE|WOODLAND')]
    # add location system based on year of data
    if args['year'] >= '2019':
        df['LocationSystem'] = 'FIPS_2019'
    elif '2015' <= args['year'] < '2019':
        df['LocationSystem'] = 'FIPS_2015'
    elif '2013' <= args['year'] < '2015':
        df['LocationSystem'] = 'FIPS_2013'
    elif '2010' <= args['year'] < '2013':
        df['LocationSystem'] = 'FIPS_2010'
    # Add hardcoded data
    df['Class'] = 'Money'
    df['SourceName'] = "USDA_CropValues_and_MeatProduction"
    df['LocationSystem'] = 'FIPS_' + args["year"]
    df['DataReliability'] = None
    df['DataCollection'] = 2
    return df



