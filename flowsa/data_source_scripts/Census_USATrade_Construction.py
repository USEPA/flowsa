# Census_USATrade.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Census USA Trade data for imports and exports by U.S. state for select
construction materials and their precursors

https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf
https://www.census.gov/data/developers/data-sets/international-trade.html

"""
import json
import pandas as pd
from flowsa.common import datapath
from flowsa.flowsa_log import log
from flowsa.location import apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def census_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the 'build_url' input from generateflowbyactivity.py,
    which is a base url for data imports/exports that requires parts of the url text
    string to be replaced with info specific to the data request. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """
    urls = []
    for flow in config['url'].get('flows'):
        for naics in config['url'].get('naics'):
            if flow == 'imports':
                get_params = config['url']['get_params'].get('import_params')
            elif flow == 'exports':
                get_params = config['url']['get_params'].get('export_params')
            get_params_list = '%2C'.join(get_params)
            replacements = {
                '__flows__': flow,
                '__naics__': naics,
                '__get_params__': get_params_list
            }
            request_url = build_url
            for placeholder, value in replacements.items():
                request_url = request_url.replace(placeholder, value) 
            urls.append(request_url)
    return urls


def census_usatrade_call(*, resp, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    if resp.status_code == 204:
        # No content warning, return empty dataframe
        log.warning(f"No content found for {resp.url}")
        return pd.DataFrame()
    census_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(
        data=census_json[1:len(census_json)], columns=census_json[0])

    df_census = (df_census
                 .assign(Type = 'exports' if 'exports' in url else 'imports'))
    
    return df_census


def census_usatrade_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # exclude country grouping codes and 'total' rows; only retain data for 
    # individual countries based on Schedule C codes
    # remove rows where CTY_CODE starts with '0', contains 'X', or equals '-'
    df = df[~(
        df['CTY_CODE'].str.startswith('0') | 
        df['CTY_CODE'].str.contains('X') | 
        (df['CTY_CODE'] == '-')
    )]
        
    df = (df
          .assign(FlowAmount = lambda x: x['CON_VAL_YR'].astype(float)
                  .fillna(x['ALL_VAL_YR'].astype(float)))
          .rename(columns={'NAICS':'FlowName',
                           'Type':'Compartment',
                           'STATE':'State',
                           'YEAR':'Year',
                           'CTY_NAME':'Description'})
          .drop(columns=['CTY_CODE', 'CON_VAL_YR', 'MONTH', 'ALL_VAL_YR'], errors='ignore')
          )

    # replace the 2-state abbreviations with FIPS code
    df = apply_county_FIPS(df).drop(columns=['State', 'County']) # defaults to FIPS 2015

    # check for duplicate rows    
    x = df.drop_duplicates(subset=['FlowName', 'Year', 'Compartment',
                                   'Location', 'Description'])
    if len(x) < len(df):
        print('ERROR check duplicates')

    # add hard coded data
    df['Class'] = 'Money'
    df['SourceName'] = 'Census_USATrade'
    df['Unit'] = 'USD'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['LocationSystem'] = 'FIPS_2015'
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    
    return df


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_USATrade_Construction', year=2024)
    fba = flowsa.getFlowByActivity('Census_USATrade_Construction', 2015)
