# datapull.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
"""
import yaml
import requests
import json
from flowsa.common import outputpath, sourceconfigpath, log, local_storage_path,\
    flow_by_activity_fields

def store_flowbyactivity(result, source):
    """Prints the data frame into a parquet file."""
    try:
        result.to_parquet(outputpath + source +'.parquet', 'pyarrow')
    except:
        log.error('Failed to save '+source+' file.')

def make_http_request(url):
    try:
        r = requests.get(url)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    return r

def load_sourceconfig(source):
    sfile = sourceconfigpath+source+'.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config

def load_api_key(api_source):
    """
    Loads a txt file from the appdirs user directory with a set name
    in the form of the host name and '_API_KEY.txt' like 'BEA_API_KEY.txt'
    containing the users personal API key. The user must register with this
    API and get the key and save it to a .txt file in the user directory specified
    by local_storage_path (see common.py for definition)
    :param api_source: str, name of source, like 'BEA' or 'Census'
    :return: the users API key as a string
    """
    keyfile = local_storage_path+'/'+ api_source + '_API_KEY.txt'
    try:
        with open(keyfile,mode='r') as keyfilecontents:
            key = keyfilecontents.read()
    except IOError:
        log.error("Key file not found.")
    return key

def load_json_from_requests_response(response_w_json):
    response_json = json.loads(response_w_json.text)
    return response_json

def add_missing_flow_by_activity_fields(flowbyactivity_partial_df):
    """
    Add in missing fields to have a complete and ordered
    :param flowbyactivity_partial_df:
    :return:
    """
    for k in flow_by_activity_fields.keys():
        if k not in flowbyactivity_partial_df.columns:
            flowbyactivity_partial_df[k]=None
    #Resort it so order is correct
    flowbyactivity_partial_df = flowbyactivity_partial_df[flow_by_activity_fields.keys()]
    return flowbyactivity_partial_df



