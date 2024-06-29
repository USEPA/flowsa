# Census_ACS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls American Community Survey 5-yr Data Profile (DP03): Selected Economic Characteristics
--year = 'year' e.g. 2015
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


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


def DP03_5yr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    # rename columns to increase readibility
    df = df.rename(columns={'DP03_0062E':'MHI',
                            'DP03_0128PE':'Percent Below Poverty Line',
                            'DP03_0009PE':'Unemployment Rate',
                            'NAME': 'County Name',
                            'GEO_ID':'Location'})

    # melt economic columns into one FlowAmount column
    df = df.melt(id_vars= ['Location'], 
                 value_vars=['MHI', 'Percent Below Poverty Line', 'Unemployment Rate'],
                 var_name='FlowName',
                 value_name='FlowAmount')
    
    # assign units based on the FlowName values
    df.loc[df.FlowName == 'MHI', 'Unit'] = 'USD'
    df.loc[df.FlowName == 'Percent Below Poverty Line', 'Unit'] = '% of people'
    df.loc[df.FlowName == 'Unemployment Rate', 'Unit'] = '% of unemployed people' #as a percentage of the civilian labor force


    # hard code data for flowsa format
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_DP03_5yr'
    df['Description'] = 'Economic data from 5yr DP03'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df

def DP04_5yr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    if year in ['2010','2011','2012','2013','2014']:
        df = df.rename(columns={'DP04_0045PE':'FlowAmount', 
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    else:
        df = df.rename(columns={'DP04_0046PE':'FlowAmount',
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    
    # hard code data for flowsa format
    df['FlowName'] = 'Owner-occupied housing'
    df['Unit'] = '% of homes' # percent of occupied homes
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_DP04_5yr'
    df['Description'] = 'Housing data from 5yr DP04'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df

def DP05_5yr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    # rename columns to increase readibility
    df = df.rename(columns={'DP05_0001E':'FlowAmount', #Total Population, ACS
                            'NAME': 'County Name',
                            'GEO_ID':'Location'})
    
    
    # hard code data for flowsa format
    df['FlowName'] = 'Total Population'
    df['Unit'] = '# of people'
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_DP05_5yr'
    df['Description'] = 'Total Population data from 5yr DP05'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df

def S1501_5yr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    if year in ['2010','2011','2012','2013','2014']:
        df = df.rename(columns={'S1501_C01_014E':'High School Degree',
                                'S1501_C01_015E':'Bachelors Degree',
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    else:
        df = df.rename(columns={'S1501_C02_014E':'High School Degree',
                                'S1501_C02_015E':'Bachelors Degree',
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    
    # melt economic columns into one FlowAmount column
    df = df.melt(id_vars= ['Location'], 
                 value_vars=['High School Degree', 'Bachelors Degree'],
                 var_name='FlowName',
                 value_name='FlowAmount')
    
    # assign units based on the FlowName values
    df.loc[df.FlowName == 'High School Degree', 'Unit'] = '% of people'
    df.loc[df.FlowName == 'Bachelors Degree', 'Unit'] = '% of people'
    
    # hard code data for flowsa format
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_S1501_5yr'
    df['Description'] = 'Educational Attainment data from 5yr S1501'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_DP03_5yr', year=2018)
    fba = flowsa.getFlowByActivity('Census_DP03_5yr', year=2018)
