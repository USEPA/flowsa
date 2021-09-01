# flowbyactivity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
File configuration requires a year for the data pull and a data
source (yaml file name) as parameters
EX: --year 2015 --source USGS_NWIS_WU
"""

import requests
import argparse
import pandas as pd
from esupy.processed_data_mgmt import write_df_to_file
from flowsa.common import log, make_http_request, load_api_key, load_sourceconfig, \
    paths, rename_log_file
from flowsa.metadata import set_fb_meta, write_metadata
from flowsa.flowbyfunctions import flow_by_activity_fields, fba_fill_na_dict, \
    dynamically_import_fxn
from flowsa.dataclean import clean_df


def parse_args():
    """
    Make year and source script parameters
    :return: dictionary, 'year' and 'source'
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("-y", "--year", required=True, help="Year for data pull and save")
    ap.add_argument("-s", "--source", required=True, help="Data source code to pull and save")
    args = vars(ap.parse_args())
    return args


def set_fba_name(datasource, year):
    """
    Generate name of FBA used when saving parquet
    :param datasource: str, datasource
    :param year: str, year
    :return: str, name of parquet
    """
    if year is not None:
        name_data = datasource + "_" + str(year)
    else:
        name_data = datasource
    return name_data


def build_url_for_query(config, args):
    """
    Creates a base url which requires string substitutions that depend on data source
    :param config: dictionary, FBA yaml
    :param args: dictionary, load parameters 'source' and 'year'
    :return: base url used to load data
    """
    # if there are url parameters defined in the yaml, then build a url, else use "base_url"
    urlinfo = config["url"]
    if urlinfo != 'None':
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
            build_url = build_url.replace("__year__", str(args["year"]))
        if "__apiKey__" in build_url:
            userAPIKey = load_api_key(config['api_name'])  # (common.py fxn)
            build_url = build_url.replace("__apiKey__", userAPIKey)
        return build_url


def assemble_urls_for_query(build_url, config, args):
    """
    Calls on helper functions defined in source.py files to replace parts of the url string
    :param build_url: str, base url
    :param config: dictionary, FBA yaml
    :param args: dictionary, load parameters 'source' and 'year'
    :return: list, urls to call data from
    """

    if "url_replace_fxn" in config:
        # dynamically import and call on function
        urls = dynamically_import_fxn(args['source'],
                                      config["url_replace_fxn"])(build_url=build_url,
                                                                 config=config, args=args)
    else:
        urls = []
        urls.append(build_url)
    return urls


def call_urls(url_list, args, config):
    """
    This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data.
    The processing method is specific to
    the data source, so this function relies on a function in source.py
    :param url_list: list, urls to call
    :param args: dictionary, load parameters 'source' and 'year'
    :param config: dictionary, FBA yaml
    :return: list, dfs to concat and parse
    """
    # start requests session
    s = requests.Session()
    # identify if url request requires cookies set
    if 'allow_http_request_cookies' in config:
        set_cookies = 'yes'
    else:
        set_cookies = 'no'

    # create dataframes list by iterating through url list
    data_frames_list = []
    if url_list[0] is not None:
        for url in url_list:
            log.info("Calling %s", url)
            r = make_http_request(url, requests_session=s, set_cookies=set_cookies)
            if "call_response_fxn" in config:
                # dynamically import and call on function
                df = dynamically_import_fxn(args['source'], config["call_response_fxn"])(url=url, r=r, args=args)
            if isinstance(df, pd.DataFrame):
                data_frames_list.append(df)
            elif isinstance(df, list):
                data_frames_list.extend(df)

    return data_frames_list


def parse_data(dataframe_list, args, config):
    """
    Calls on functions defined in source.py files, as parsing rules are specific to the data source.
    :param dataframe_list: list, dfs to concat and parse
    :param args: dictionary, load parameters 'source' and 'year'
    :param config: dictionary, FBA yaml
    :return: df, single df formatted to FBA
    """
    # if hasattr(sys.modules[__name__], config["parse_response_fxn"]):
    if "parse_response_fxn" in config:
        # dynamically import and call on function
        df = dynamically_import_fxn(args['source'],
                                    config["parse_response_fxn"])(dataframe_list=dataframe_list,
                                                                  args=args)
    return df


def process_data_frame(df, source, year, config):
    """
    Process the given dataframe, cleaning, converting data, and writing the final parquet.
    This method was written to move code into a shared method, which was necessary to support
    the processing of a list of dataframes instead of a single dataframe.
    :param df: df, FBA format
    :param source: str, source name
    :param year: str, year
    :return: df, FBA format, standardized
    """
    # log that data was retrieved
    log.info("Retrieved data for %s %s", source, year)
    # add any missing columns of data and cast to appropriate data type
    log.info("Add any missing columns and check field datatypes")
    flow_df = clean_df(df, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)
    # sort df and reset index
    flow_df = flow_df.sort_values(['Class', 'Location', 'ActivityProducedBy', 'ActivityConsumedBy',
                                   'FlowName', 'Compartment']).reset_index(drop=True)
    # save as parquet file
    name_data = set_fba_name(source, year)
    meta = set_fb_meta(name_data, "FlowByActivity")
    write_df_to_file(flow_df,paths,meta)
    write_metadata(source, config, meta, "FlowByActivity", year=year)
    log.info("FBA generated and saved for %s", name_data)
    # rename the log file saved to local directory
    rename_log_file(name_data, meta)


def main(**kwargs):
    """
    Generate FBA parquet(s)
    :param kwargs: 'source' and 'year'
    :return: parquet saved to local directory
    """
    # assign arguments
    if len(kwargs) == 0:
        kwargs = parse_args()

    # assign yaml parameters (common.py fxn)
    config = load_sourceconfig(kwargs['source'])

    log.info("Creating dataframe list")
    # @@@01082021JS - Range of years defined, to support split into multiple Parquets:
    if '-' in str(kwargs['year']):
        years = str(kwargs['year']).split('-')
        min_year = int(years[0])
        max_year = int(years[1]) + 1
        year_iter = list(range(min_year, max_year))
    else:
        # Else only a single year defined, create an array of one:
        year_iter = [kwargs['year']]

    for p_year in year_iter:
        kwargs['year'] = str(p_year)
        # build the base url with strings that will be replaced
        build_url = build_url_for_query(config, kwargs)
        # replace parts of urls with specific instructions from source.py
        urls = assemble_urls_for_query(build_url, config, kwargs)
        # create a list with data from all source urls
        dataframe_list = call_urls(urls, kwargs, config)
        # concat the dataframes and parse data with specific instructions from source.py
        log.info("Concat dataframe list and parse data")
        df = parse_data(dataframe_list, kwargs, config)
        if isinstance(df, list):
            for frame in df:
                if not len(frame.index) == 0:
                    try:
                        source_names = frame['SourceName']
                        source_name = source_names.iloc[0]
                    except KeyError:
                        source_name = kwargs['source']
                    process_data_frame(frame, source_name, kwargs['year'], config)
        else:
            process_data_frame(df, kwargs['source'], kwargs['year'], config)


if __name__ == '__main__':
    main()
