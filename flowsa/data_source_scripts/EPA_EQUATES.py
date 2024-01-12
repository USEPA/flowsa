# EPA_EQUATES.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls EPA Air QUAlity TimE Series (EQUATES) Project data
"""

import io
from zipfile import ZipFile
from os import path
import pandas as pd
from urllib import parse
from tempfile import TemporaryFile, NamedTemporaryFile
import shutil
import tarfile
from io import BytesIO
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.dataclean import standardize_units


def equates_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    params = {'id': config['url']['file_id_dict'][int(year)]}
    return [f'{build_url}&{parse.urlencode(params)}']


def equates_call(*, resp, year, config, **_):
    """
    Convert response to pandas dataframe
    :param resp: response from url call
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: pandas dataframe of original source data
    """
    with TemporaryFile() as temp:
        temp.write(resp.content)
        temp.seek(0)
        with tarfile.open(fileobj=temp, mode='r:gz') as tar:
            tar.getmembers()  # Index the tarball
            df_list = [pd.read_csv(BytesIO(tar.extractfile(file).read()),
                                   comment='#')
                       for file in config['file_list'][int(year)]]
            return pd.concat(df_list)


def equates_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = (pd.concat(df_list, sort=True)
          .rename(columns=config['parse']['rename_columns']))

    # drop all other columns
    df.drop(columns=df.columns.difference(['FlowName',
                                           'FlowAmount',
                                           'ActivityProducedBy',
                                           'Location',
                                           'Unit',
                                           'Description']), inplace=True)

    # make sure FIPS are string and 5 digits
    df['Location'] = df['Location'].astype('str').apply('{:0>5}'.format)
    # remove records from certain FIPS
    df = df[~df['Location'].str.match(config['parse']['excluded_fips'])]
    # Drop hazardous pollutants, (HAPs), as the EQUATES team did not check
    # these for consistency or completeness.
    df = df[df['Description'].str[:].isin(config['parse']['pollutant_list'])]

    # to align with other processed NEI data (Point from StEWI), units are
    # converted during FBA creation instead of maintained
    df['Unit'] = 'TON'
    df = standardize_units(df)
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Class'] = "Chemicals"
    df['SourceName'] = source
    df['Compartment'] = "air"
    df['Year'] = year
    df['DataReliability'] = 3
    df['DataCollection'] = 5
    df = assign_fips_location_system(df, year)

    return df
