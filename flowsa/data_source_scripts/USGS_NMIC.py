# USGS_NMIC.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
USGS National Minerals Information Center
https://www.usgs.gov/centers/national-minerals-information-center/historical-statistics-mineral-and-material-commodities
"""

import io
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system


def usgs_nmic_url_build(*, build_url, config, **_):
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
    # Replace year-dependent aspects of file url
    for mineral, m_dict in config['minerals'].items():
        url = (build_url
               .replace('__FILE__', m_dict.get('file'))
               )
        urls.append(url)
    return urls

def usgs_nmic_call(*, resp, url, year, config, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # convert response to dataframe
    df_load = pd.read_excel(io.BytesIO(resp.content),
                            skiprows=4,
                            skipfooter=3,
                            )
    # assign mineral name to df based on file specified in yaml
    inv = {v['file']: k for k,v in config['minerals'].items()}
    material = inv.get(url.rsplit('/', 1)[-1], 'ERROR')
    df = df_load.assign(material = material)
    
    df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)

    return df


def usgs_nmic_parse(*, df_list, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    parsed_list = []
    for df in df_list:
        
        df= df.melt( id_vars=['Year','material'], var_name= 'ActivityProducedBy', value_name='FlowAmount', ignore_index= False)
        df= df.rename(columns={'material':'FlowName'})     
        df = (df
              .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy'].str.strip())
              .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy'] + ', '+ x['FlowName'])
             # [~df.ActivityProducedBy.str.contains('$')]
              )
                
        ## format the dfs

        parsed_list.append(df)
    df = pd.concat(parsed_list, ignore_index=True)

    df = assign_fips_location_system(df, 2024)
    # hard code data
    df['Year'] = df['Year'].astype(str)
    df['SourceName'] = 'USGS_NMIC'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Unit'] = 'MT'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = 'resource'
    df['Class'] = 'Chemicals'
    return df


if __name__ == "__main__":
    import flowsa
    year = '2012-2022'
    flowsa.generateflowbyactivity.main(source='USGS_NMIC', year=year)
    fba = flowsa.getFlowByActivity('USGS_NMIC', year=2015)
    