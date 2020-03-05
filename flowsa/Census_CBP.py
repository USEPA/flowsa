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
import argparse
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request,\
    load_json_from_requests_response, load_api_key, add_missing_flow_by_activity_fields
from flowsa.common import log, flow_by_activity_fields, get_all_state_FIPS_2

#Make year a script parameter
ap = argparse.ArgumentParser()
ap.add_argument("-y","--year", required=True, help="Year for data pull and save")
args = vars(ap.parse_args())

source = 'Census_CBP'

def build_url_for_api_query(urlinfo):
    params = ""
    for k,v in urlinfo['url_params'].items():
        params = params+'&'+k+"="+str(v)

    url = "{0}{1}{2}".format(urlinfo['base_url'],urlinfo['url_path'],params)
    return url

def assemble_urls_for_api_query():
    urls = []
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
    for c in FIPS_2:
        url = build_url_for_api_query(config['url'])
        url = url + c
        # add key to url
        url = url + '&key=' + load_api_key("Census")
        urls.append(url)
    return urls

def call_cbp_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in url_list:
        log.INFO("Calling "+url)
        r = make_http_request(url)
        cbp_json = load_json_from_requests_response(r)
        # Convert response
        df = pd.DataFrame(data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
        data_frames_list.append(df)
    return data_frames_list

cbp_flow_specific_metadata = \
    {"EMP": {"Class": "Employment",
             "FlowName": "Number of employees",
             "SourceName": source + "_EMP",
             "Unit": "p"},
     "ESTAB": {"Class": "Other",
               "FlowName": "Number of establishments",
               "SourceName": source + "_ESTAB",
               "Unit": "p"},
     "PAYANN": {"Class": "Money",
                "FlowName": "Annual payroll",
                "SourceName": source + "_PAYANN",
                "Unit": "1000USD"},
     }

if __name__ == '__main__':
    config = load_sourceconfig(source)
    # Part of Census CBP API URL is dynamic based on the year
    config['url']['url_path'] = args["year"] + '/cbp?'
    urls = assemble_urls_for_api_query()
    df_list = call_cbp_urls(urls)
    df = pd.concat(df_list)
    #convert county='999' to line for full state
    df.loc[df['county']=='999','county'] = '000'
    #Make FIPS as a combo of state and county codes
    df['FIPS'] = df['state']+df['county']
    #now drop them
    df = df.drop(columns=['state','county'])
    #drop all sectors record
    df = df[df['NAICS2012']!="00"]
    #Rename fields
    df = df.rename(columns={"NAICS2012":"ActivityProducedBy"})
    #Add year
    df['Year'] = args["year"]
    #Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment']=None

    # Split the table into 3 for three flow types, add custom field, and store each
    dfs = {}
    for k,v in cbp_flow_specific_metadata.items():
        flow_df = df
        flow_df['FlowAmount']=flow_df[k]
        drop_fields = list(cbp_flow_specific_metadata.keys())
        flow_df = df.drop(columns=drop_fields)
        for k2,v2 in v.items():
            flow_df[k2]=v2
        flow_df = add_missing_flow_by_activity_fields(flow_df)
        dfs[k] = flow_df
        parquet_name = cbp_flow_specific_metadata[k]["SourceName"]+'_'+args['year']
        store_flowbyactivity(flow_df, parquet_name)
     ##TO DO
    #Check for spread data
    #Score data quality
