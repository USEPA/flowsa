# Census_ACS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
U.S. Census American Community Survey
5-yr Data Profile (DP03): Selected Economic Characteristics
5-yr Data Profile (DP04): Selected Housing Characteristics
5-yr Data Profile (DP05): Demographic and Housing Estimates
5-yr Data Profile (S1501): Educational Attainment

variables: https://api.census.gov/data/2022/acs/acs5/profile/variables.html
"""
import json
import pandas as pd
import numpy as np
from esupy.remote import make_url_request
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def load_acs_variables():
    data = make_url_request('https://api.census.gov/data/2022/acs/acs5/profile/variables.json')
    variables = json.loads(data.content).get('variables')
    return variables

def DP_URL_helper(*, build_url, year, **_):
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
    urls = []

    url = build_url
    # url = url.replace("%3A%2A", ":*")
    urls.append(url)

    return urls


def DP_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    json_data = json.loads(resp.text)
    # convert response to dataframe
    df = pd.DataFrame(data=json_data[1:len(json_data)],
                      columns=json_data[0])
    return df

def parse_years(year):
    if '-' in str(year):
        years = str(year).split('-')
        year_iter = list(range(int(years[0]), int(years[1]) + 1))
        year_iter = [str(x) for x in year_iter]
    return year_iter


def DP_5yr_parse(*, df_list, config, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # limit variable dictionary for select years
    year_dict = {k: v for k,v in config.get('variables').items()
                 if year in parse_years(v.get('years', [year]))}
    description = {k: v['label'] for k,v in load_acs_variables().items()
                   if k in year_dict.keys()}
    names = {k: v['name'] for k,v in year_dict.items()}
    units = {k: v['unit'] for k,v in year_dict.items()}

    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    # melt economic columns into one FlowAmount column
    df = (df.melt(id_vars= ['GEO_ID'], 
                  value_vars=year_dict.keys(),
                  var_name='code',
                  value_name='FlowAmount')
          .assign(FlowName = lambda x: x['code'].map(names))
          .assign(Unit = lambda x: x['code'].map(units))
          .assign(Description = lambda x: x['code'].map(description))
          .assign(Description = lambda x: 
                  x[['code', 'Description']].agg(': '.join, axis=1)
                  .str.replace('!!', ', '))
          .rename(columns={'GEO_ID':'Location'})
          )

    # hard code data for flowsa format
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_ACS'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_DP03_5yr', year=2018)
    fba = flowsa.getFlowByActivity('Census_DP03_5yr', year=2018)
    flowsa.generateflowbyactivity.main(source='Census_DP04_5yr', year=2018)
    fba2 = flowsa.getFlowByActivity('Census_DP04_5yr', year=2018)
    flowsa.generateflowbyactivity.main(source='Census_DP05_5yr', year=2018)
    fba3 = flowsa.getFlowByActivity('Census_DP05_5yr', year=2018)
    flowsa.generateflowbyactivity.main(source='Census_DP05_5yr', year=2018)
    fba3 = flowsa.getFlowByActivity('Census_DP05_5yr', year=2018)
