# flowbyactivity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
File configuration requires a year for the data pull and a data source (yaml file name) as parameters
EX: --year 2015 --source USGS_NWIS_WU
"""

import argparse
from flowsa.common import *
from flowsa.flowbyfunctions import add_missing_flow_by_fields, clean_df, fba_fill_na_dict
from flowsa.Blackhurst_IO import *
from flowsa.BLS_QCEW import *
from flowsa.Census_CBP import *
from flowsa.Census_AHS import *
from flowsa.Census_PEP_Population import *
from flowsa.EIA_CBECS_Water import *
from flowsa.EPA_NEI import *
from flowsa.StatCan_GDP import *
from flowsa.StatCan_IWS_MI import *
from flowsa.StatCan_LFS import *
from flowsa.USDA_CoA_Cropland import *
from flowsa.USDA_CoA_Cropland_NAICS import *
from flowsa.USDA_CoA_Livestock import *
from flowsa.USDA_ERS_FIWS import *
from flowsa.USDA_IWMS import *
from flowsa.USGS_NWIS_WU import *
from flowsa.USDA_ERS_MLU import *
from flowsa.EIA_CBECS_Land import *
from flowsa.EIA_CBECS_Water import *
from flowsa.EIA_MECS import *
from flowsa.BLM_PLS import *
from flowsa.EIA_MER import *
from flowsa.EPA_GHG_Inventory import *



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
        f = fbaoutputpath + source + "_" + str(year) + '.parquet'
    else:
        f = fbaoutputpath + source + '.parquet'
    try:
        result.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save '+source + "_" + str(year) +' file.')


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
        userAPIKey = load_api_key(config['api_name'])  # (common.py fxn)
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


def call_urls(url_list, args):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data. The processing method is specific to
    the data source, so this function relies on a function in source.py"""

    data_frames_list = []
    for url in url_list:
        log.info("Calling " + url)
        r = make_http_request(url)
        if hasattr(sys.modules[__name__], config["call_response_fxn"]):
            df = getattr(sys.modules[__name__], config["call_response_fxn"])(url, r, args)
        if isinstance(df, pd.DataFrame):
            data_frames_list.append(df)
        elif isinstance(df, list):
            data_frames_list.extend(df)

    return data_frames_list


def parse_data(dataframe_list, args):
    """Calls on functions defined in source.py files, as parsing rules are specific to the data source."""
    if hasattr(sys.modules[__name__], config["parse_response_fxn"]):
        df = getattr(sys.modules[__name__], config["parse_response_fxn"])(dataframe_list, args)
        return df


if __name__ == '__main__':
    # assign arguments
    args = parse_args()
    # assign yaml parameters (common.py fxn)
    config = load_sourceconfig(args['source'])
    log.info("Creating dataframe list")
    # build the base url with strings that will be replaced
    build_url = build_url_for_query(config['url'])
    # replace parts of urls with specific instructions from source.py
    urls = assemble_urls_for_query(build_url, config, args)
    # create a list with data from all source urls
    dataframe_list = call_urls(urls, args)
    # concat the dataframes and parse data with specific instructions from source.py
    log.info("Concat dataframe list and parse data")
    df = parse_data(dataframe_list, args)
    # log that data was retrieved
    log.info("Retrieved data for " + args['source'] + ' ' + args['year'])
    # add any missing columns of data and cast to appropriate data type
    log.info("Add any missing columns and check field datatypes")
    flow_df = clean_df(df, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)
    # modify flow units
    flow_df = convert_fba_unit(flow_df)
    # sort df and reset index
    flow_df = flow_df.sort_values(['Class', 'Location', 'ActivityProducedBy', 'ActivityConsumedBy',
                                   'FlowName', 'Compartment']).reset_index(drop=True)
    # save as parquet file
    log.info('Save dataframe as parquet')
    parquet_name = args['source'] + '_' + args['year']
    store_flowbyactivity(flow_df, parquet_name)

