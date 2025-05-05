# BEA_IEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Data for imports and exports from BEA International Economic Accounts
"""

import json
import pandas as pd
from flowsa.common import datapath


def bea_iea_url_helper(*, build_url, config, year, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    urls = []
    country_dict = get_country_schema()
    ctys = [value for key, value in country_dict.items() if value != '1000']
    for cty in ctys:
        request_url = build_url.replace('__areaorcountry__', cty)
        urls.append(request_url)

    return urls


def get_country_schema():
    """
    Acquires the concordance between countries across ISO country codes and 
    BEA service imports countries (strings with their API name equivalents)
    """
    country_dict = (pd.read_csv(datapath / 'BEA_country_names.csv')
                       .filter(['BEA_AREAORCOUNTRY', 'country'])
                       .drop_duplicates()
                       .set_index('country')['BEA_AREAORCOUNTRY']
                       .to_dict()
                       )
    return country_dict


def bea_iea_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    try:
        json_load = json.loads(resp.text)
        df = pd.DataFrame(data=json_load['BEAAPI']['Results']['Data'])
    except:
        df = pd.DataFrame()
    finally:
        return df


def bea_iea_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, ignore_index=True)
    country_dict0 = get_country_schema()
    country_dict = {v:k for k,v in country_dict0.items()}

    df = (df.
          rename(columns={'AreaOrCountry': 'Location',
                          'CL_UNIT': 'Unit',
                          'TypeOfService': 'ActivityProducedBy',
                          'TimeSeriesDescription': 'Description',
                          'TradeDirection': 'FlowName',
                          })
          .assign(FlowAmount = lambda x: pd.to_numeric(x['DataValue']).fillna(0) * 1000000)
          .assign(Location = lambda x: x['Location'].map(country_dict))
          .drop(columns=['UNIT_MULT', 'Affiliation', 'DataValue',
                         'TimeSeriesId', 'TimePeriod'], errors='ignore')
          )

    # add hard code data
    df['SourceName'] = 'BEA_IEA'
    df['Class'] = 'Money'
    df['ActivityConsumdBy'] = ''
    df['LocationSystem'] = 'BEA Countries'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['FlowType'] = "TECHNOSPHERE_FLOW"

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='BEA_IEA', year=2023)
    fba = pd.DataFrame()
    for y in range(2023, 2024):
        fba = pd.concat([fba, flowsa.getFlowByActivity('BEA_IEA', y)],
                        ignore_index=True)
