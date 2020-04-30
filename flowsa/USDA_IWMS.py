# USDA_IWMS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
from flowsa.common import *


def iwms_url_helper(build_url, config, args):
    """This helper function uses the "build_url" input from datapull.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls_iwms = []

    # replace "__aggLevel__" in build_url to create two urls
    for x in config['agg_levels']:
        url = build_url
        url = url.replace("__aggLevel__", x)
        url = url.replace(" ", "%20")
        urls_iwms.append(url)
    return urls_iwms


def iwms_call(url, response, args):
    iwms_json = json.loads(response.text)
    # Convert response to dataframe
    df_iwms = pd.DataFrame(data=iwms_json["data"])
    return df_iwms


def iwms_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria and only includes data on harvested acreage
    (irrigated and total)."""
    df = pd.concat(dataframe_list, sort=True)
    # only interested in total water applied, not water applied by type of irrigation
    df = df[df['domain_desc'] == 'TOTAL']
    # drop unused columns
    df = df.drop(columns=['CV (%)', 'agg_level_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc',
                          'util_practice_desc'])
    # combine FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['FIPS'] = df['state_fips_code'] + df['county_code']
    df.loc[df['FIPS'] == '99000', 'FIPS'] = US_FIPS  # modify national level fips
    # combine column information to create activity information, and create two new columns for activities
    df['ActivityConsumedBy'] = df['commodity_desc'] + ', ' + df['class_desc']  # drop this column later
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(", ALL CLASSES", "", regex=True)  # not interested in all data from class_desc
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount",
                            "unit_desc": "Unit",
                            "year": "Year",
                            "short_desc": "Description",
                            "domaincat_desc": "Compartment"})
    # drop remaining unused columns
    df = df.drop(columns=['class_desc', 'commodity_desc', 'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc', 'domain_desc'])
    # modify contents of flowamount column, "D" is supressed data, "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = withdrawn_keyword
    # df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # # Add hardcoded data
    df['Class'] = "Water"
    df['SourceName'] = "USDA_IWMS"
    df['DataReliability'] = None  #TODO score data qualtiy
    df['DataCollection'] = None
    return df
