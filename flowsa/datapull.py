# datapull.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
File configuration requires a year for the data pull and a data source (yaml file name) as parameters
"""
import pandas as pd
import argparse
import yaml
import requests
import json


from flowsa.common import outputpath, sourceconfigpath, log, local_storage_path, \
    flow_by_activity_fields, get_all_state_FIPS_2
from flowsa.flowbyactivity import add_missing_flow_by_activity_fields


from flowsa.USDA_CoA_Cropland import *
from flowsa.USDA_CoA_Livestock import *
from flowsa.USDA_IWMS import *
from flowsa.USGS_Water_Use import *
from flowsa.BLS_QCEW import *
from flowsa.Census_CBP import *
from flowsa.USDA_CoA_ProdMarkValue import *
#from flowsa.EIA_CBECS import *


def parse_args():
    """Make year and source script parameters"""
    ap = argparse.ArgumentParser()
    ap.add_argument("-y", "--year", required=True, help="Year for data pull and save")
    ap.add_argument("-s", "--source", required=True, help="Data source code to pull and save")
    args = vars(ap.parse_args())
    return args


def store_flowbyactivity(result, source, year=None):
    """Prints the data frame into a parquet file."""
    if year is not None:
        f = outputpath + source + "_" + str(year) + '.parquet'
    else:
        f = outputpath + source + '.parquet'
    try:
        result.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save '+source + "_" + str(year) +' file.')


def make_http_request(url):
    r = []
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
    keyfile = local_storage_path + '/' + api_source + '_API_KEY.txt'
    key = ""
    try:
        with open(keyfile, mode='r') as keyfilecontents:
            key = keyfilecontents.read()
    except IOError:
        log.error("Key file not found.")
    return key


def build_url_for_query(urlinfo):
    """Creates a base url which requires string substitutions that depend on data source"""
    # if there are url parameters defined in the yaml, then build a url, else use "base_url"
    if 'url_params' in urlinfo:
        params = ""
        for k, v in urlinfo['url_params'].items():
            params = params+'&'+k+"="+str(v)

    if 'url_params' in urlinfo:
        build_url = "{0}{1}{2}".format(urlinfo['base_url'], urlinfo['api_path'], params)
    else:
        build_url = "{0}".format(urlinfo['base_url'])

    # substitute year from arguments and users api key into the url
    if "__year__" in build_url:
        build_url = build_url.replace("__year__", args["year"])
    if "__apiKey__" in build_url:
        userAPIKey = load_api_key(config['api_name'])
        build_url = build_url.replace("__apiKey__", userAPIKey)
    return build_url


def assemble_urls_for_query(build_url, config, args):
    """Calls on helper functions defined in source.py files to replace parts of the url string"""
    if "url_replace_fxn" in config:
        if hasattr(sys.modules[__name__], config["url_replace_fxn"]):
            urls = getattr(sys.modules[__name__], config["url_replace_fxn"])(build_url, config, args)
    else:
        urls = []
        urls.append(build_url)
    return urls



def call_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data. The processing method is specific to
    the data source, so this function relies on a function in source.py"""
    data_frames_list = []
    for url in url_list:
        log.info("Calling " + url)
        r = make_http_request(url)
        if hasattr(sys.modules[__name__], config["call_response_fxn"]):
            df = getattr(sys.modules[__name__], config["call_response_fxn"])(url, r)
        data_frames_list.append(df)
    return data_frames_list


def parse_data(dataframe_list, args):
    """Calls on functions defined in source.py files, as parsing rules are specific to the data source."""
    if hasattr(sys.modules[__name__], config["parse_response_fxn"]):
        df = getattr(sys.modules[__name__], config["parse_response_fxn"])(dataframe_list, args)
        return df


if __name__ == '__main__':
    # assign arguments
    args = parse_args()
    # assign yaml parameters
    config = load_sourceconfig(args['source'])
    # build the base url with strings that will be replaced
    build_url = build_url_for_query(config['url'])
    # replace parts of urls with specific instructions from source.py
    urls = assemble_urls_for_query(build_url, config, args)
    # create a list with data from all source urls
    dataframe_list = call_urls(urls)
    # concat the dataframes and parse data with specific instructions from source.py
    df = parse_data(dataframe_list, args)
    # log that data was retrieved
    log.info("Retrieved data for " + args['source'])
    # if there is specific metadata in source.py, call on it to split dataframe into multiple dataframes
    if 'flow_metadata' in config:
        if hasattr(sys.modules[__name__], config["flow_metadata"]):
            md = getattr(sys.modules[__name__], config["flow_metadata"])
            dfs = {}
            for k, v in md.items():
                flow_df = df
                flow_df['FlowAmount'] = flow_df[k]
                drop_fields = list(md.keys())
                flow_df = df.drop(columns=drop_fields)
                for k2, v2 in v.items():
                    flow_df[k2] = v2
                # add missing dataframe fields (also converts columns to desired datatype)
                flow_df = add_missing_flow_by_activity_fields(flow_df)
                dfs[k] = flow_df
                parquet_name = args['source'] + "_" + str(k) + '_' + args['year']
                store_flowbyactivity(flow_df, parquet_name)
    # if there isn't source specific metadata, add missing flowbyactivity fields and store data as parquet
    else:
        # add missing dataframe fields (also converts columns to desired datatype)
        flow_df = add_missing_flow_by_activity_fields(df)
        parquet_name = args['source'] + '_' + args['year']
        store_flowbyactivity(flow_df, parquet_name)








