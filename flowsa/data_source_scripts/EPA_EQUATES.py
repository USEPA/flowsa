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
from flowsa.common import convert_fba_unit


def equates_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
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
                       for file in config['file_list'][year]]
            return pd.concat(df_list)


def equates_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
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
    df = convert_fba_unit(df)
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Class'] = "Chemicals"
    df['SourceName'] = source
    df['Compartment'] = "air"
    df['Year'] = year
    df['DataReliability'] = 3
    df['DataCollection'] = 5
    df = assign_fips_location_system(df, year)

    return df


def clean_NEI_fba(fba):
    """
    Clean up the NEI FBA for use in FBS creation
    :param fba: df, FBA format
    :return: df, modified FBA
    """
    fba = remove_duplicate_NEI_flows(fba)
    fba = drop_GHGs(fba)
    # Remove the portion of PM10 that is PM2.5 to eliminate double counting,
    # rename FlowName and Flowable, and update UUID
    fba = remove_flow_overlap(fba, 'PM10 Primary (Filt + Cond)',
                              ['PM2.5 Primary (Filt + Cond)'])
    # # link to FEDEFL
    # import fedelemflowlist
    # mapping = fedelemflowlist.get_flowmapping('NEI')
    # PM_df = mapping[['TargetFlowName',
    #                  'TargetFlowUUID']][mapping['SourceFlowName']=='PM10-PM2.5']
    # PM_list = PM_df.values.flatten().tolist()
    PM_list = ['Particulate matter, > 2.5μm and ≤ 10μm',
               'a320e284-d276-3167-89b3-19d790081c08']
    fba.loc[(fba['FlowName'] == 'PM10 Primary (Filt + Cond)'),
            ['FlowName', 'Flowable', 'FlowUUID']] = ['PM10-PM2.5',
                                                     PM_list[0], PM_list[1]]
    return fba


def clean_NEI_fba_no_pesticides(fba):
    """
    Clean up the NEI FBA with no pesicides for use in FBS creation
    :param fba: df, FBA format
    :return: df, modified FBA
    """
    fba = drop_pesticides(fba)
    fba = clean_NEI_fba(fba)
    return fba


def remove_duplicate_NEI_flows(df):
    """
    These flows for PM will get mapped to the primary PM flowable in FEDEFL
    resulting in duplicate emissions
    :param df: df, FBA format
    :return: df, FBA format with duplicate flows dropped
    """
    flowlist = [
        'PM10-Primary from certain diesel engines',
        'PM25-Primary from certain diesel engines',
    ]

    df = df.loc[~df['FlowName'].isin(flowlist)]
    return df


def drop_GHGs(df):
    """
    GHGs are included in some NEI datasets. If these data are not
    compiled together with GHGRP, need to remove them as they will be
    tracked from a different source
    :param df: df, FBA format
    :return: df
    """""
    # Flow names reflect source data prior to FEDEFL mapping, using 'FlowName'
    # instead of 'Flowable'
    flowlist = [
        'Carbon Dioxide',
        'Methane',
        'Nitrous Oxide',
        'Sulfur Hexafluoride',
    ]

    df = df.loc[~df['FlowName'].isin(flowlist)]

    return df


def drop_pesticides(df):
    """
    To avoid overlap with other datasets, emissions of pesticides
    from pesticide application are removed.
    :param df: df, FBA format
    :return: df
    """
    # Flow names reflect source data prior to FEDEFL mapping, using 'FlowName'
    # instead of 'Flowable'
    flowlist = [
        '2,4-Dichlorophenoxy Acetic Acid',
        'Captan',
        'Carbaryl',
        'Methyl Bromide',
        'Methyl Iodide',
        'Parathion',
        'Trifluralin',
    ]

    activity_list = [
        '2461800001',
        '2461800002',
        '2461850000',
    ]

    df = df.loc[~(df['FlowName'].isin(flowlist) &
                  df['ActivityProducedBy'].isin(activity_list))]

    return df


def remove_flow_overlap(df, aggregate_flow, contributing_flows):
    """
    Quantity of contributing flows is subtracted from aggregate flow and the
    aggregate flow quantity is updated. Modeled after function of same name in
    stewicombo.overlaphandler.py
    :param df: df, FBA format
    :param aggregate_flow: str, flowname to modify
    :param contributing_flows: list, flownames contributing to aggregate flow
    :return: df, FBA format, modified flows
    """
    match_conditions = ['ActivityProducedBy', 'Compartment',
                        'Location', 'Year']

    df_contributing_flows = df.loc[df['FlowName'].isin(contributing_flows)]
    df_contributing_flows = df_contributing_flows.groupby(
        match_conditions, as_index=False)['FlowAmount'].sum()

    df_contributing_flows['FlowName'] = aggregate_flow
    df_contributing_flows['ContributingAmount'] = \
        df_contributing_flows['FlowAmount']
    df_contributing_flows.drop(columns=['FlowAmount'], inplace=True)
    df = df.merge(df_contributing_flows, how='left',
                  on=match_conditions.append('FlowName'))
    df[['ContributingAmount']] = df[['ContributingAmount']].fillna(value=0)
    df['FlowAmount'] = df['FlowAmount'] - df['ContributingAmount']
    df.drop(columns=['ContributingAmount'], inplace=True)

    # Make sure the aggregate flow is non-negative
    df.loc[((df.FlowName == aggregate_flow) & (df.FlowAmount <= 0)),
           "FlowAmount"] = 0
    return df
