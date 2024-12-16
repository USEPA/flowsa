# generateflowbyactivity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
File configuration requires a year for the data pull and a data
source (yaml file name) as parameters
EX: --year 2015 --source USGS_NWIS_WU
"""

import argparse
import pandas as pd
from urllib import parse
import time
import flowsa
from esupy.processed_data_mgmt import write_df_to_file
from esupy.remote import make_url_request
from flowsa.common import load_env_file_key, sourceconfigpath, \
    load_yaml_dict, get_flowsa_base_name
from flowsa.settings import paths
from flowsa.flowsa_log import log, reset_log_file
from flowsa.metadata import set_fb_meta, write_metadata
from flowsa.schema import flow_by_activity_fields
from flowsa.dataclean import clean_df


def parse_args():
    """
    Make year and source script parameters
    :return: dictionary, 'year' and 'source'
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("-y", "--year", required=True,
                    help="Year for data pull and save")
    ap.add_argument("-s", "--source", required=True,
                    help="Data source code to pull and save")
    args = vars(ap.parse_args())
    return args


def set_fba_name(source, year):
    """
    Generate name of FBA used when saving parquet
    :param source: str, source
    :param year: str, year
    :return: str, name of parquet
    """
    return source if year is None else f'{source}_{year}'


def assemble_urls_for_query(*, source, year, config):
    """
    Calls on helper functions defined in source.py files to
    replace parts of the url string
    :param source: str, data source
    :param year: str, year
    :param config: dictionary, FBA yaml
    :return: list, urls to call data from
    """
    # if there are url parameters defined in the yaml,
    # then build a url, else use "base_url"
    urlinfo = config.get('url', 'None')
    if urlinfo == 'None':
        return [None]

    if 'url_params' in urlinfo:
        params = parse.urlencode(urlinfo['url_params'], safe='=&%',
                                 quote_via=parse.quote)
        build_url = urlinfo['base_url'] + urlinfo['api_path'] + params
    else:
        build_url = urlinfo['base_url']

    # substitute year from arguments and users api key into the url
    build_url = build_url.replace("__year__", str(year))
    if "__apiKey__" in build_url:
        userAPIKey = load_env_file_key('API_Key', config['api_name'])
        build_url = build_url.replace("__apiKey__", userAPIKey)

    fxn = config.get("url_replace_fxn")
    if callable(fxn):
        urls = fxn(build_url=build_url, source=source,
                   year=year, config=config)
        return urls
    elif fxn:
        raise flowsa.exceptions.FBSMethodConstructionError(
            error_type='fxn_call')
    else:
        return [build_url]


def call_urls(*, url_list, source, year, config):
    """
    This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data.
    The processing method is specific to
    the data source, so this function relies on a function in source.py
    :param url_list: list, urls to call
    :param source: str, data source
    :param year: str, year
    :param config: dictionary, FBA yaml
    :return: list, dfs to concat and parse
    """
    # identify if url request requires cookies set
    set_cookies = config.get('allow_http_request_cookies')
    confirm_gdrive = config.get('confirm_gdrive')
    pause = config.get('time_delay', 0) # in seconds

    # create dataframes list by iterating through url list
    data_frames_list = []
    if url_list[0] is not None:
        for url in url_list:
            df = None
            log.info("Calling %s", url)
            resp = make_url_request(url,
                                    set_cookies=set_cookies,
                                    confirm_gdrive=confirm_gdrive)
            fxn = config.get("call_response_fxn")
            if callable(fxn):
                df = fxn(resp=resp, source=source, year=year,
                        config=config, url=url)
            elif fxn:
                raise flowsa.exceptions.FBSMethodConstructionError(
                    error_type='fxn_call')
            if isinstance(df, pd.DataFrame):
                data_frames_list.append(df)
            elif isinstance(df, list):
                data_frames_list.extend(df)
            time.sleep(pause)

    return data_frames_list


def parse_data(*, df_list, source, year, config):
    """
    Calls on functions defined in source.py files, as parsing rules
    are specific to the data source.
    :param df_list: list, dfs to concat and parse
    :param source: str, data source
    :param year: str, year
    :param config: dictionary, FBA yaml
    :return: df, single df formatted to FBA
    """

    fxn = config.get("parse_response_fxn")
    if callable(fxn):
        df = fxn(df_list=df_list, source=source, year=year, config=config)
    elif fxn:
        raise flowsa.exceptions.FBSMethodConstructionError(
            error_type='fxn_call')
    # else:
    #   Handle parse_response_fxn = None
    return df


def process_data_frame(*, df, source, year, config):
    """
    Process the given dataframe, cleaning, converting data, and
    writing the final parquet. This method was written to move code into a
    shared method, which was necessary to support the processing of a list
    of dataframes instead of a single dataframe.
    :param df: df, FBA format
    :param source: str, source name
    :param year: str, year
    :param config: dict, items in method yaml
    :return: df, FBA format, standardized
    """
    # log that data was retrieved
    log.info("Retrieved data for %s %s", source, year)
    # add any missing columns of data and cast to appropriate data type
    log.info("Add any missing columns and check field datatypes")
    flow_df = clean_df(df, flow_by_activity_fields,
                       drop_description=False)
    # sort df and reset index
    flow_df = flow_df.sort_values(['Class', 'Location', 'ActivityProducedBy',
                                   'ActivityConsumedBy', 'FlowName',
                                   'Compartment']).reset_index(drop=True)
    # save as parquet file
    name_data = set_fba_name(source, year)
    meta = set_fb_meta(name_data, "FlowByActivity")
    write_df_to_file(flow_df, paths, meta)
    write_metadata(source, config, meta, "FlowByActivity", year=year)
    log.info("FBA generated and saved for %s", name_data)
    # rename the log file saved to local directory
    reset_log_file(name_data, meta)


def main(**kwargs):
    """
    Generate FBA parquet(s)
    :param kwargs: 'source' and 'year'
    :return: parquet saved to local directory
    """
    # assign arguments
    if len(kwargs) == 0:
        kwargs = parse_args()

    source = kwargs['source']
    year = kwargs['year']

    # assign yaml parameters (common.py fxn), drop any extensions to FBA
    # filename if run into error
    try:
        config = load_yaml_dict(source, flowbytype='FBA')
    except FileNotFoundError:
        log.info(f'Could not find Flow-By-Activity config file for {source}')
        source = get_flowsa_base_name(sourceconfigpath, source, "yaml")
        log.info(f'Generating FBA for {source}')
        config = load_yaml_dict(source, flowbytype='FBA')

    log.info("Creating dataframe list")
    # year input can either be sequential years (e.g. 2007-2009) or single year
    if '-' in str(year):
        years = str(year).split('-')
        year_iter = list(range(int(years[0]), int(years[1]) + 1))
    else:
        # Else only a single year defined, create an array of one:
        year_iter = [year]

    # check that year(s) are listed in the method yaml, return warning if not
    years_list = list(set(list(map(int, year_iter))
                          ).difference(config['years']))
    if len(years_list) != 0:
        log.warning(f'Years not listed in FBA method yaml: {years_list}, '
                    f'data might not exist')

    if config.get('call_all_years'):
        urls = assemble_urls_for_query(source=source, year=None, config=config)
        df_list = call_urls(url_list=urls, source=source, year=None, config=config)
        dfs = parse_data(df_list=df_list, source=source, year=None, config=config)
        call_all_years = True
    else:
        call_all_years = False
    for p_year in year_iter:
        year = str(p_year)
        if not call_all_years:
            # replace parts of urls with specific instructions from source.py
            urls = assemble_urls_for_query(source=source,
                                           year=year, config=config)
            # create a list with data from all source urls
            df_list = call_urls(url_list=urls,
                                source=source, year=year, config=config)
            # concat the dataframes and parse data with specific
            # instructions from source.py
            log.info("Concat dataframe list and parse data")
            dfs = parse_data(df_list=df_list, source=source,
                             year=year, config=config)
        if isinstance(dfs, list):
            for frame in dfs:
                if not len(frame.index) == 0:
                    try:
                        source_names = frame['SourceName']
                        source_name = source_names.iloc[0]
                    except KeyError:
                        source_name = source
                    process_data_frame(df=frame,
                                       source=source_name, year=year,
                                       config=config)
        elif call_all_years:
            process_data_frame(
                df=dfs.query('Year == @year').reset_index(drop=True),
                source=source, year=year, config=config)

        else:
            process_data_frame(df=dfs, source=source, year=year, config=config)


if __name__ == '__main__':
    main()
