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
from flowsa.datapull import * #make_http_request, load_json_from_requests_response
from flowsa.common import * #log, flow_by_activity_fields, get_all_state_FIPS_2


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


def census_cbp_call(dat_json):
    cbp_json = dat_json
    # convert response to dataframe
    df_census = pd.DataFrame(data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
    return df_census


def census_cbp_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
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
    # Add year
    df['Year'] = args["year"]
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