# -*- coding: utf-8 -*-
"""
EPA REI
"""
import re
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowbysector import FlowBySector


def rei_url_helper(*, build_url, config, **_):
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
    # initiate url list for coa cropland data
    urls = []
    # replace "__xlsx_name__" in build_url to create three urls
    for x in config['files']:
        url = build_url
        url = url.replace("__filename__", x)
        urls.append(url)
    return urls


def rei_call(*, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv(url)
    # append filename for use in parsing
    fn = re.search('sourcedata/REI_(.*).csv', url)
    df['Description'] = fn.group(1)

    return df


def primary_factors_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year of FBS
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    rei_list = []

    for df in df_list:
        # df for primary factors
        if df['Description'][0] == 'primaryfactors':
            df.iloc[0, 1] = 'ActivityProducedBy'
            df = (df
                  .drop(df.columns[0], axis=1)
                  .rename(columns=df.iloc[0])
                  .rename(columns={'primaryfactors': 'Description'})
                  .drop(df.index[0])
                  .reset_index(drop=True)
                  )

            # use "melt" fxn to convert columns into rows
            df = df.melt(id_vars=["Description", "ActivityProducedBy"],
                         var_name="FlowName",
                         value_name="FlowAmount")
            df["Class"] = "Money"
            df.loc[df['FlowName'] == 'Employment', 'Class'] = 'Employment'
            df["FlowType"] = 'ELEMENTARY_FLOW'
            df['Unit'] = 'Thousand USD'
            df.loc[df['FlowName'] == 'Employment', 'Unit'] = 'p'
            df['FlowAmount'] = df['FlowAmount'].astype(float)

        # df for waste - sector consumed by
        elif df['Description'][0] == 'useintersection':
            df.iloc[0, 1] = 'FlowName'
            df = (df
                  .drop(df.columns[0], axis=1)
                  .rename(columns=df.iloc[0])
                  .rename(columns={'useintersection': 'Description'})
                  .drop(df.index[0])
                  .reset_index(drop=True)
                  )

            df = (df
                  .drop(columns=df.columns[[1]])
                  .drop([0, 1])  # Drop blanks
                  .melt(id_vars=['Description', 'FlowName'],
                        var_name='ActivityConsumedBy',
                        value_name='FlowAmount')
                  )
            df = df[~df['FlowAmount'].astype(str).str.contains(
                '- ')].reset_index(drop=True)
            df['FlowAmount'] = (df['FlowAmount'].str.replace(
                ',', '').astype('float'))
            df['Unit'] = 'USD'
            # Where recyclable code >= 9 (Gleaned product), change units to MT
            df.loc[df['FlowName'].str.contains('metric tons'), 'Unit'] = 'MT'
            df['FlowName'] = df['FlowName'].apply(
                lambda x: x.split('(', 1)[0])
            df["Class"] = "Other"
            df['FlowType'] = 'WASTE_FLOW'
        # df for waste - sector produced by
        elif df['Description'][0] == 'makecol':
            df = (df
                  .drop(df.columns[0], axis=1)
                  .rename(columns={'Unnamed: 1': 'ActivityProducedBy'})
                  .drop(df.index[0])
                  .reset_index(drop=True)
                  )
            # Assign final row as Post-consumer
            df['ActivityProducedBy'].iloc[-1] = 'Estimate from Post-Consumer Waste'
            df = (df
                  .melt(id_vars=['Description', 'ActivityProducedBy'],
                        var_name='FlowName',
                        value_name='FlowAmount')
                  .dropna()
                  )
            df = df[~df['FlowAmount'].astype(str).str.contains(
                '-')].reset_index(drop=True)
            df['FlowAmount'] = (df['FlowAmount'].str.replace(
                ',', '').astype('float'))
            df['Unit'] = 'MT'
            df['Unit'] = np.where(df['FlowName'].str.contains('\$'), 'USD',
                                  df['Unit'])
            df['FlowName'] = df['FlowName'].apply(
                lambda x: x.split('(', 1)[0])
            df["Class"] = "Other"
            df['FlowType'] = 'WASTE_FLOW'
        rei_list.append(df)

    df2 = pd.concat(rei_list, sort=False)

    # update employment to jobs
    df2['FlowName'] = np.where(df2['FlowName'] == 'Employment', 'Jobs',
                               df2['FlowName'])

    # address trailing white space
    string_cols = list(df2.select_dtypes(include=['object', 'int']).columns)
    for s in string_cols:
        df2[s] = df2[s].str.strip()

    # hardcode info
    df2['Location'] = US_FIPS
    df2 = assign_fips_location_system(df2, year)
    df2['SourceName'] = 'EPA_REI'
    df2["Year"] = year
    df2['DataReliability'] = 5
    df2['DataCollection'] = 5

    return df2


def rei_waste_national_cleanup(fbs: FlowBySector, **_) -> FlowBySector:
    """
    Drop imports/exports from rei waste national FBS
    """
    fbs = (fbs
           .query('SectorConsumedBy not in ("F04000", "F05000")')
           .reset_index(drop=True)
          )

    return fbs
