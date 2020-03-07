# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls QCEW data from BLS
"""

import pandas as pd
import io
import argparse
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request,\
    load_json_from_requests_response, load_api_key, add_missing_flow_by_activity_fields
from flowsa.common import log, flow_by_activity_fields, read_stored_FIPS, get_all_state_FIPS_2

#Make year a script parameter
ap = argparse.ArgumentParser()
ap.add_argument("-y", "--year", required=True, help="Year for data pull and save")
args = vars(ap.parse_args())

source = 'BLS_QCEW'

def build_url_for_api_query(urlinfo):
    url = "{0}{1}".format(urlinfo['base_url'],urlinfo['url_path'])
    return url

def assemble_urls_for_api_query():
    urls = []
    #FIPS = read_stored_FIPS()['FIPS'][1:]
    FIPS = get_all_state_FIPS_2()['FIPS_2']
    for c in FIPS:
        url = build_url_for_api_query(config['url'])
        url = url + c + '000.csv'
        urls.append(url)
    return urls

def call_qcew_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in url_list:
        log.info("Calling "+url)
        r = make_http_request(url).content
        df = pd.read_csv(io.StringIO(r.decode('utf-8')))[["area_fips", "own_code",
                                                          "industry_code", "year",
                                                          "annual_avg_estabs",
                                                          "annual_avg_emplvl"]]
        data_frames_list.append(df)
    return data_frames_list

qcew_flow_specific_metadata = \
    {"EMP": {"Class": "Employment",
             "FlowName": "Number of employees",
             "SourceName": source + "_EMP",
             "Unit": "p"},
     "ESTAB": {"Class": "Other",
               "FlowName": "Number of establishments",
               "SourceName": source + "_ESTAB",
               "Unit": "p"},
     }

if __name__ == '__main__':
    config = load_sourceconfig(source)
    # Part of BLS QCEW URL is dynamic based on the year
    config['url']['url_path'] = args["year"] + '/a/area/'
    urls = assemble_urls_for_api_query()
    df_list = call_qcew_urls(urls)
    df = pd.concat(df_list)
    # keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin([1, 2, 3, 5])]
    # aggregate annual_avg_estabs and annual_avg_emplvl by area_fips, industry_code, year, flag
    df = df.groupby(["area_fips",
                     "industry_code",
                     "year"])[["annual_avg_estabs", "annual_avg_emplvl"]].sum().reset_index()
    #Rename fields
    df = df.rename(columns={"area_fips":"FIPS",
                            "industry_code":"ActivityProducedBy",
                            "year":"Year",
                            "annual_avg_estabs":"ESTAB",
                            "annual_avg_emplvl":"EMP"})
    # adjust area_fips in QCEW
    if len(str(df.FIPS[1])) == 4:
        df.FIPS = str(0) + str(df.FIPS[1])
    #Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment']=None
    # Split the table into 3 for three flow types, add custom field, and store each
    dfs = {}
    for k,v in qcew_flow_specific_metadata.items():
        flow_df = df
        flow_df['FlowAmount']=flow_df[k]
        drop_fields = list(qcew_flow_specific_metadata.keys())
        flow_df = df.drop(columns=drop_fields)
        for k2,v2 in v.items():
            flow_df[k2]=v2
        flow_df = add_missing_flow_by_activity_fields(flow_df)
        dfs[k] = flow_df
        parquet_name = qcew_flow_specific_metadata[k]["SourceName"]+'_'+args['year']
        store_flowbyactivity(flow_df, parquet_name)
     ##TO DO
    #Check for spread data
    #Score data quality