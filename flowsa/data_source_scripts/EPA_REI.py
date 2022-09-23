# -*- coding: utf-8 -*-
"""
EPA REI
"""
import re
import pandas as pd
from flowsa.location import US_FIPS


def rei_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
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
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv(url)
    # append filename for use in parsing
    fn = re.search('sourcedata/REI_(.*).csv', url)
    df['filename'] = fn.group(1)

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
        print(f"now testing.{df['filename'][0]}.")
        # df for primary factors
        if df['filename'][0] == 'primaryfactors':
            df = df.drop(columns='filename')
            df.iloc[0, 0] = 'Description'
            df.iloc[0, 1] = 'ActivityProducedBy'
            df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)

            # use "melt" fxn to convert columns into rows
            df = df.melt(id_vars=["Description", "ActivityProducedBy"],
                         var_name="FlowName",
                         value_name="FlowAmount")
            df["Class"] = "Money"
            df.loc[df['FlowName'] == 'Employment', 'Class'] = 'Employment'
            df['Unit'] = 'Thousand USD'
            df.loc[df['FlowName'] == 'Employment', 'Unit'] = 'p'

        # df for waste - sector consumed by
        elif df['filename'][0] == 'useintersection':
            df = df.drop(columns='filename')
            df.iloc[0, 0] = 'Description'
            df.iloc[0, 1] = 'FlowName'
            df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(
                drop=True)

            df = (df
                  .drop(columns=df.columns[[2]])
                  .drop([0, 1])  # Drop blanks
                  .melt(id_vars=['Description', 'FlowName'],
                        var_name='ActivityConsumedBy',
                        value_name='FlowAmount')
                  )
            df = df[~df['FlowAmount'].str.contains('-')].reset_index(
                drop=True)
            df['FlowAmount'] = (df['FlowAmount'].str.replace(
                ',', '').astype('float'))
            df['Unit'] = 'USD'
            # Where recyclable code >= 9 (Gleaned product), change units to MT
            df.loc[df['Description'].str[-2:].astype('int') >= 9,
                   'Unit'] = 'MT'
            df['FlowName'] = df['FlowName'].apply(
                lambda x: x.split('(', 1)[0])
            df["Class"] = "Other"
            df['FlowType'] = 'WASTE_FLOW'
        # df for waste - sector produced by
        elif df['filename'][0] == 'makecol':
            df = df.drop(columns='filename')
            df.iloc[0, 0] = 'Description'
        rei_list.append(df)

    df2 = pd.concat(rei_list, sort=False)

    # hardcode info
    df2['Location'] = US_FIPS
    df2['SourceName'] = 'EPA_REI'
    df2["Year"] = year
    df2['DataReliability'] = 5
    df2['DataCollection'] = 5

    return df2


