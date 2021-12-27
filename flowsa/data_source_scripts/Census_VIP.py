# Census_VIP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
U.S. Census Value of Construction Put in Place
https://www.census.gov/construction/c30/c30index.html
"""
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.common import US_FIPS


def census_vip_call(url, response_load, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: url: string, url
    :param kwargs: response_load: df, response from url call
    :param kwargs: args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    df = pd.read_excel(response_load.content,
                       sheet_name = 'Total',
                       header = 3).dropna().reset_index(drop=True)
    
    df.loc[df['Type of Construction:'].str.startswith("  "),
            'Type of Construction:'] = 'Nonresidential - ' + df['Type of Construction:'].str.strip()
    df = df[df['Type of Construction:'] != 'Nonresidential']
    index_1 = np.where(df['Type of Construction:'].str.startswith(
        "Total Private Construction"))[0][0]
    index_2 = np.where(df['Type of Construction:'].str.startswith(
        "Total Public Construction"))[0][0]

    df_private = df.iloc[index_1+1:index_2, :]
    df_public = df.iloc[index_2+1:, :]
    
    #TODO  fix setting with copy warning
    df_public['Type of Construction:'] = 'Public, ' + df_public['Type of Construction:']
    df_private['Type of Construction:'] = 'Private, ' + df_private['Type of Construction:']
    
    df2 = pd.concat([df_public, df_private], ignore_index=True)
    
    df2 = df2.melt(id_vars = ['Type of Construction:'],
                  var_name = 'Year',
                  value_name = 'FlowAmount')

    return df2
    

def census_vip_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    df = pd.concat(dataframe_list, sort=False)
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'] == args['year']].reset_index(drop=True)
    df = df.rename(columns = {'Type of Construction:':'ActivityProducedBy'})

    df['Class'] = 'Money'
    df['SourceName'] = 'Census_VIP'
    df['FlowName'] = 'Construction spending'
    # millions of dollars
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    df['Unit'] = 'USD'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Compartment'] = None
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['Year'] = args['year']
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    
    return df

