# Census_EC.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls U.S. Census Bureau Economic Census Data
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def census_EC_URL_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    for k, v in config['datasets'].items():
        for dataset in v.get(year, []):
            url = (build_url
                   .replace('__dataset__', k)
                   .replace('__group__', f'group({dataset})')
                   )
            if year == '2012':
                # for 2012 need both us and state call separately
                url += '&for=us:*'
                urls_census.append(url)
                url = url.replace('&for=us:*', '&for=state:*')
                urls_census.append(url)
            else:
                urls_census.append(url)

    return urls_census


def census_EC_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    census_json = json.loads(resp.text)
    url = resp.url
    desc = url[url.find("(")+1:url.find(")")] # extract the group from the url
    # convert response to dataframe
    df = pd.DataFrame(data=census_json[1:len(census_json)],
                      columns=census_json[0])
    df = df.assign(Description = desc)
    return df


def census_EC_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    if(year == '2017'):
        df = (df
              .query('TAXSTAT_LABEL == "All establishments"')
              .query('TYPOP_LABEL == "All establishments"')
              )
        class_label = 'CLASSCUST_LABEL'
    else:
        class_label = 'CLASSCUST_TTL'

    df = (df
          .filter([f'NAICS{year}', class_label, 'ESTAB', 'RCPTOT', 'RCPTOT_F',
                   'GEO_ID', 'RCPTOT_DIST', 'YEAR', 'Description'])
          .rename(columns={f'NAICS{year}': 'ActivityProducedBy',
                           f'{class_label}': 'ActivityConsumedBy',
                           'ESTAB': 'Number of establishments',
                           'RCPTOT': 'Sales, value of shipments, or revenue',
                           'RCPTOT_DIST': 'Distribution of sales, value of shipments, or revenue',
                           'RCPTOT_F': 'Note',
                           'YEAR': 'Year'})
          .assign(Location = lambda x: x['GEO_ID'].str[-2:])
          .melt(id_vars=['ActivityProducedBy', 'ActivityConsumedBy',
                         'Location', 'Year', 'Description', 'Note',],
                value_vars=['Number of establishments',
                            'Sales, value of shipments, or revenue',
                            'Distribution of sales, value of shipments, or revenue'],
                value_name='FlowAmount',
                var_name='FlowName')
          .assign(FlowAmount = lambda x: x['FlowAmount'].astype(float))
          )

    # Updated suppressed data field
    df = (df.assign(
        Suppressed = np.where(df.Note.isin(["D"]),
                              df.Note, np.nan),
        FlowAmount = np.where(df.Note.isin(["D"]),
                              0, df.FlowAmount))
            .drop(columns='Note'))

    conditions = [df['FlowName'] == 'Number of establishments',
                  df['FlowName'] == 'Sales, value of shipments, or revenue',
                  df['FlowName'] == 'Distribution of sales, value of shipments, or revenue']
    df['Unit'] = np.select(conditions, ['p', 'USD', 'Percent'])
    df['Class'] = np.select(conditions, ['Other', 'Money', 'Money'])
    df['FlowAmount'] = np.where(df['FlowName'] == 'Sales, value of shipments, or revenue',
                                df['FlowAmount'] * 1000,
                                df['FlowAmount'])
    df['Location'] = np.where(df['Location'] == 'US', US_FIPS,
                              df['Location'].str.pad(5, side='right', fillchar='0'))

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_EC'
    df['FlowType'] = "ELEMENTARY_FLOW"
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_EC', year=2017)
    fba = flowsa.getFlowByActivity('Census_EC', 2017)
