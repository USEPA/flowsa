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
import numpy as np

import flowsa.flowbyactivity
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.dataclean import standardize_units
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowsa_log import log
from flowsa.location import merge_urb_cnty_pct


def epa_nei_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
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
           .replace('__version__', version_dict[year])
           .replace('__suffix__', config['url']
                    .get('suffix', {})
                    .get(year, config['url']
                         .get('suffix', {})
                         .get('base', ''))
                    )
           )
    return [url]


def epa_nei_call(*, resp, **_):
    """
    Convert response for calling _1 to pandas dataframe
    :param _1: string, url (unused)
    :param resp: df, response from url call
    :param _2: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source') (unused)
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
    # drop all other columns
    df = df.drop(columns=df.columns.difference(
                 list(config['col_dict'][year].keys())))

    # make sure FIPS are string and 5 digits
    df = (df.assign(Location=lambda x:
                    x['Location'].fillna(0)
                                 .astype('int')
                                 .astype('str')
                                 .str.zfill(5))
            )
    # remove records from certain FIPS
    excluded_fips = ['78', '85', '88']
    df = df[~df['Location'].str[0:2].isin(excluded_fips)]
    excluded_fips2 = ['777']
    df = df[~df['Location'].str[-3:].isin(excluded_fips2)]

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


def clean_NEI_fba(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    Clean up the NEI FBA for use in FBS creation
    :param fba: df, FBA format
    :return: modified FBA
    """
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    # Remove the portion of PM10 that is PM2.5 to eliminate double counting,
    # rename resulting FlowName
    fba = remove_flow_overlap(fba, 'PM10 Primary (Filt + Cond)',
                              ['PM2.5 Primary (Filt + Cond)'])

    fba['FlowName'] = np.where(fba['FlowName'] == 'PM10 Primary (Filt + Cond)',
                               "PM10-PM2.5",
                               fba['FlowName'])
    # Drop zero values to reduce size
    fba = fba.query('FlowAmount != 0').reset_index(drop=True)

    apply_urban_rural = fba.config.get('apply_urban_rural', False)
    if apply_urban_rural:
        log.info(f'Splitting {fba.full_name} into urban and rural '
                 'quantities by FIPS.')
        fba = merge_urb_cnty_pct(fba)

    new_fba = FlowByActivity(fba)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])
    # to reduce the file size of the FBA and avoid memory errors, consolidate
    # to geoscale (i.e., state) early on
    new_fba = new_fba.convert_to_geoscale()

    return new_fba


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

if __name__ == '__main__':
    import flowsa
    flowsa.generateflowbyactivity.main(source='EPA_NEI_Onroad', year='2020')
    fba = flowsa.flowbyactivity.getFlowByActivity('EPA_NEI_Onroad', '2020')
