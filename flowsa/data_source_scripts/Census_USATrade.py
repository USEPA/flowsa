# Census_USATrade.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Census USA Trade data for imports ande exports by NAICS

https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf
https://www.census.gov/data/developers/data-sets/international-trade.html

"""
import json
import pandas as pd
from flowsa.common import datapath
from flowsa.flowsa_log import log


def census_url_helper(*, build_url, year, config, **_):
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
    country_dict = get_country_schema()
    ctys = [value for key, value in country_dict.items() if value != '1000']
    for cty in ctys:
        request_url = build_url.replace('__areaorcountry__', cty)
        for flow in config['url'].get('flows'):
            if flow == 'exports':
                urls.append(request_url
                            .replace('__flows__', flow)
                            .replace('GEN_CIF_YR', 'ALL_VAL_YR'))
            elif flow == 'imports':
                urls.append(request_url.replace('__flows__', flow))
    return urls


def get_country_schema():
    """
    Generates a a concordance between ISO codes and Census country codes (4-digit)
    """
    l = []
    with open(datapath / 'Census_country_codes.txt') as f:
        for line in f:
            a = line.split('|')
            l2 = []
            for item in a:
                l2.append(item.strip())
            if len(l2)>=3:
                l.append(l2)
    headers = l[0]
    df = pd.DataFrame(l, columns=headers)
    df = df.iloc[1:,:]
    df = df.rename(columns={'Code':'Census Code'})

    country_dict = (df
                    .set_index('Name')['Census Code']
                    .to_dict()
                    )
    return country_dict


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

    country_dict0 = get_country_schema()
    country_dict = {v:k for k,v in country_dict0.items()}

    df = (df
          .assign(FlowAmount = lambda x: x['GEN_CIF_YR'].astype(float)
                  .fillna(x['ALL_VAL_YR'].astype(float)))
          .assign(Location = lambda x: x['CTY_CODE'].map(country_dict))
          .assign(FlowName = lambda x: x['Type'])
          .rename(columns={'YEAR': 'Year',
                           'NAICS': 'ActivityProducedBy'})
          .drop(columns=['MONTH', 'COMM_LVL', 'CTY_CODE', 'GEN_CIF_YR', 'Type',
                         'ALL_VAL_YR'], errors='ignore')
          .assign(Unit='USD')
          .assign(SourceName='Census_USATrade')
          )

    x = df.drop_duplicates(subset=['ActivityProducedBy', 'Year',
                                   'Location', 'FlowName'])
    if len(x) < len(df):
        print('ERROR check duplicates')

    # add hard code data
    df['Class'] = 'Money'
    df['ActivityConsumdBy'] = ''
    df['LocationSystem'] = 'Census Countries'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['FlowType'] = "TECHNOSPHERE_FLOW"

    return df


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_USATrade', year=2022)
    fba = flowsa.getFlowByActivity('Census_USATrade', 2022)
