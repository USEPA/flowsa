# EPA_NEI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls EPA National Emissions Inventory (NEI) data for nonpoint sources
"""

import io
from zipfile import ZipFile
from os import path
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.dataclean import standardize_units


def epa_nei_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    version_dict = config['version_dict']
    url = (build_url
           .replace('__year__', year)
           .replace('__version__', version_dict[year]))

    return [url]


def epa_nei_call(*, resp, **_):
    """
    Convert response for calling _1 to pandas dataframe
    :param _1: string, url (unused)
    :param resp: df, response from url call
    :param _2: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source') (unused)
    :return: pandas dataframe of original source data
    """
    with ZipFile(io.BytesIO(resp.content)) as z:
        # Read in all .csv files from the zip archive as a list of dataframes
        df_list = [pd.read_csv(z.open(name)) for name in z.namelist()
                   if path.splitext(name)[1] == '.csv']
    return pd.concat(df_list)


def epa_nei_global_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=True)

    # rename columns to match flowbyactivity format
    col_dict = {value: key for (key, value) in config['col_dict'][year].items()}
    df = df.rename(columns=col_dict)

    # make sure FIPS are string and 5 digits
    df['Location'] = df['Location'].astype('str').apply('{:0>5}'.format)
    # remove records from certain FIPS
    excluded_fips = ['78', '85', '88']
    df = df[~df['Location'].str[0:2].isin(excluded_fips)]
    excluded_fips2 = ['777']
    df = df[~df['Location'].str[-3:].isin(excluded_fips2)]

    # drop all other columns
    df.drop(columns=df.columns.difference(
        list(config['col_dict'][year].keys())), inplace=True)

    # to align with other processed NEI data (Point from StEWI), units are
    # converted during FBA creation instead of maintained
    df = standardize_units(df)

    # add hardcoded data
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Class'] = "Chemicals"
    df['SourceName'] = source
    df['Compartment'] = "air"
    df['Year'] = year
    df = assign_fips_location_system(df, year)

    return df


def epa_nei_onroad_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = epa_nei_global_parse(df_list=df_list, source=source,
                              year=year, config=config)

    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 1

    return df


def epa_nei_nonroad_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """

    df = epa_nei_global_parse(df_list=df_list, source=source,
                              year=year, config=config)

    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 1

    return df


def epa_nei_nonpoint_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """

    df = epa_nei_global_parse(df_list=df_list, source=source,
                              year=year, config=config)

    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 5  # data collection scores are updated in fbs as
    # a function of facility coverage from point source data

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
