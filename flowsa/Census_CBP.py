# Census_CBP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls County Business Patterns data in NAICS from the Census Bureau
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import pandas as pd
import json
#from flowsa.datapull import build_url, make_http_request, load_from_requests_response
from flowsa.common import log, flow_by_activity_fields, get_all_state_FIPS_2, datapath


def Census_CBP_URL_helper(build_url, config, args):
    urls_census = []
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
    for c in FIPS_2:
        url = build_url
        url = url.replace("__stateFIPS__", c)
        # specified NAICS code year depends on year of data
        if args["year"] in ['2017']:
            url = url.replace("__NAICS__", "NAICS2017")
        if args["year"] in ['2012', '2013', '2014', '2015', '2016']:
            url = url.replace("__NAICS__", "NAICS2012")
        if args["year"] in ['2010', '2011']:
            url = url.replace("__NAICS__", "NAICS2007")
        urls_census.append(url)
    return urls_census


def census_cbp_call(url, cbp_response):
    cbp_json = json.loads(cbp_response.text)
    #cbp_json = cbp_dat
    # convert response to dataframe
    df_census = pd.DataFrame(data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
    return df_census


def census_cbp_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # Add year
    df['Year'] = args["year"]
    # add naics crosswalk dataframe and rename columns to match cbp, drop naics with more than 6 digits
    naics_df = pd.read_csv(datapath + "NAICS_07_to_17_Crosswalk.csv", dtype=str)
    naics_df = naics_df.rename(columns={"NAICS_2007_Code": "NAICS2007", "NAICS_2012_Code": "NAICS2012",
                                        "NAICS_2017_Code": "NAICS2017"})
    naics_df = naics_df[~(naics_df.NAICS2012.str.len() > 6)]
    # convert naics codes to naics 2012 and drop ecess naics code columns
    if 'NAICS2017' in df:
        df = pd.merge(df, naics_df, how='left', left_on='NAICS2017', right_on="NAICS2017").dropna()
        df.drop(['NAICS2007', 'NAICS2017'], axis=1, inplace=True)
    # convert county='999' to line for full state
    df.loc[df['county'] == '999', 'county'] = '000'
    # Make FIPS as a combo of state and county codes
    df['FIPS'] = df['state'] + df['county']
    # now drop them
    df = df.drop(columns=['state', 'county'])
    # drop all sectors record
    df = df[df['NAICS2012'] != "00"]
    # Rename fields
    df = df.rename(columns={"NAICS2012": "ActivityProducedBy"})
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df

# TODO: convert different NAICS years to NAICS2012

cbp_flow_specific_metadata = \
    {"EMP": {"Class": "Employment",
             "FlowName": "Number of employees",
             "Unit": "p"},
     "ESTAB": {"Class": "Other",
               "FlowName": "Number of establishments",
               "Unit": "p"},
     "PAYANN": {"Class": "Money",
                "FlowName": "Annual payroll",
                "Unit": "1000USD"},
     }