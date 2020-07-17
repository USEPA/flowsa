# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
import numpy as np
import pandas as pd
from flowsa.common import *


def CoA_Cropland_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from datapull.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        for y in config['sector_levels']:
            # at national level, remove the text string calling for state acronyms
            if x == 'NATIONAL':
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__secLevel__", y)
                url = url.replace("&state_alpha=__stateAlpha__", "")
                if y == "ECONOMICS":
                    url = url.replace(
                        "AREA HARVESTED&statisticcat_desc=AREA IN PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc=AREA BEARING %26 NON-BEARING",
                        "AREA")
                else:
                    url = url.replace("&commmodity_desc=AG LAND", "")
                url = url.replace(" ", "%20")
                urls.append(url)
            else:
                # substitute in state acronyms for state and county url calls
                for z in state_abbrevs:
                    url = build_url
                    url = url.replace("__aggLevel__", x)
                    url = url.replace("__secLevel__", y)
                    url = url.replace("__stateAlpha__", z)
                    if y == "ECONOMICS":
                        url = url.replace(
                            "AREA HARVESTED&statisticcat_desc=AREA IN PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc=AREA BEARING %26 NON-BEARING",
                            "AREA")
                    else:
                        url = url.replace("&commmodity_desc=AG LAND", "")
                    url = url.replace(" ", "%20")
                    urls.append(url)
    return urls


def coa_cropland_call(url, coa_response, args):
    cropland_json = json.loads(coa_response.text)
    df_cropland = pd.DataFrame(data=cropland_json["data"])
    return df_cropland


def coa_cropland_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria and only includes data on harvested acreage
    (irrigated and total). Data is split into two parquets, one for acreage and the other for operations"""
    df = pd.concat(dataframe_list, sort=True)
    # specify desired data based on domain_desc
    df = df[df['domain_desc'].isin(['AREA HARVESTED', 'AREA IN PRODUCTION', 'TOTAL', 'AREA BEARING & NON-BEARING', 'AREA'])]
    # Many crops are listed as their own commodities as well as grouped within a broader category (for example, orange
    # trees are also part of orchards). As this dta is not needed, takes up space, and can lead to double counting if
    # included, want to drop these unused columns
    # subset dataframe into the 5 crop types and drop rows
    # crop totals: drop all data
    # field crops: don't want certain commodities and don't want detailed types of wheat, cotton, or sunflower
    df_fc = df[df['group_desc'] == 'FIELD CROPS']
    df_fc = df_fc[~df_fc['commodity_desc'].isin(['GRASSES', 'GRASSES & LEGUMES, OTHER', 'LEGUMES', 'HAY', 'HAYLAGE'])]
    df_fc = df_fc[~df_fc['class_desc'].str.contains('SPRING|WINTER|TRADITIONAL|OIL|PIMA|UPLAND', regex=True)]
    # fruit and tree nuts: only want a few commodities
    df_ftn = df[df['group_desc'] == 'FRUIT & TREE NUTS']
    df_ftn = df_ftn[df_ftn['commodity_desc'].isin(['BERRY TOTALS', 'ORCHARDS'])]
    df_ftn = df_ftn[df_ftn['class_desc'].isin(['ALL CLASSES'])]
    # horticulture: only want a few commodities
    df_h = df[df['group_desc'] == 'HORTICULTURE']
    df_h = df_h[df_h['commodity_desc'].isin(['CUT CHRISTMAS TREES', 'SHORT TERM WOODY CROPS'])]
    # vegetables: only want a few commodities
    df_v = df[df['group_desc'] == 'VEGETABLES']
    df_v = df_v[df_v['commodity_desc'].isin(['VEGETABLE TOTALS'])]
    # only want ag land in farms & land & assets
    df_fla = df[df['group_desc'] == 'FARMS & LAND & ASSETS']
    df_fla = df_fla[df_fla['short_desc'].str.contains("AG LAND")]
    # concat data frames
    df = pd.concat([df_fc, df_ftn, df_h, df_v, df_fla])
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'domain_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips
    # use info from other columns to determine flow name
    df['FlowName'] = np.where(df["unit_desc"] == 'OPERATIONS', df["unit_desc"], df['statisticcat_desc'])
    # combine column information to create activity information, and create two new columns for activities
    df['Activity'] = df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + df['util_practice_desc']  # drop this column later
    df['Activity'] = df['Activity'].str.replace(", ALL CLASSES", "", regex=True)  # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(", ALL UTILIZATION PRACTICES", "", regex=True)  # not interested in all data from class_desc
    df['ActivityProducedBy'] = np.where(df["unit_desc"] == 'OPERATIONS', df["Activity"], 'None')
    df['ActivityConsumedBy'] = np.where(df["unit_desc"] == 'ACRES', df["Activity"], 'None')
    # add compartment based on values from other columns
    df['Compartment'] = df['prodn_practice_desc'] + ', ' + df['domaincat_desc']
    df['Compartment'] = df['Compartment'].str.replace("ALL PRODUCTION PRACTICES, ", "", regex=True)
    df['Compartment'] = df['Compartment'].str.replace("IN THE OPEN, ", "", regex=True)
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount", "unit_desc": "Unit",
                            "year": "Year", "CV (%)": "Spread",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['Activity', 'class_desc', 'commodity_desc', 'state_fips_code', 'county_code',
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
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland"
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = None
    df['DataCollection'] = 2
    return df



