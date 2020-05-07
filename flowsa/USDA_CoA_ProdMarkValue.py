# USDA_CoA_ProdMarkValue.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import json
import numpy as np
from flowsa.common import *


def pmv_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from datapull.py, which is a base url
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        for y in config['sector_levels']:
            url = build_url
            url = url.replace("__secLevel__", y)
            url = url.replace("__aggLevel__", x)
            url = url.replace(" ", "%20")
            urls.append(url)
    return urls


def pmv_call(url, pmv_response, args):
    pmv_json = json.loads(pmv_response.text)
    # Convert response to dataframe
    df_pmv = pd.DataFrame(data=pmv_json["data"])
    return df_pmv


def pmv_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria"""
    # concat data frame list into one dataframe
    df = pd.concat(dataframe_list, sort=True)
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc'])
    # combine FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips
    # combine column information to create activity information, and create two new columns for activities
    df['ActivityProducedBy'] = df['commodity_desc'] + ', ' + df['class_desc']  # drop this column later
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(", ALL CLASSES", "", regex=True)  # not interested in all data from class_desc
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "Unit",
                            "year": "Year",
                            "CV (%)": "Spread",
                            "short_desc": "Description",
                            "domain_desc": "Compartment"})
    # drop remaining unused columns
    df = df.drop(columns=['class_desc', 'commodity_desc', 'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc', 'domaincat_desc'])
    # modify contents of flowamount column, "D" is supressed data, "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # # USDA CoA 2017 states that (H) means CV >= 99.95, therefore replacing with 99.95 so can convert column to int
    # # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None # for instances where data is missing
    df.loc[df['Spread'] == "(D)", 'Spread'] = withdrawn_keyword
    # add location system based on year of data
    if args['year'] >= '2019':
        df['LocationSystem'] = 'FIPS_2019'
    elif '2015' <= args['year'] < '2019':
        df['LocationSystem'] = 'FIPS_2015'
    elif '2013' <= args['year'] < '2015':
        df['LocationSystem'] = 'FIPS_2013'
    elif '2010' <= args['year'] < '2013':
        df['LocationSystem'] = 'FIPS_2010'
    # # Add hardcoded data
    df['Class'] = "Money"
    df['SourceName'] = "USDA_CoA"
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = None
    df['DataCollection'] = None
    return df
