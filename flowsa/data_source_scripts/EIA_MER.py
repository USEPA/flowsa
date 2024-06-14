# EIA_MER.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
EIA Energy Monthly Data, summed to yearly
https://www.eia.gov/totalenergy/data/monthly/
2010 - 2020
Last updated: September 8, 2020
"""

import io
import re
import pandas as pd
import numpy as np
from esupy.mapping import apply_flow_mapping
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowbyactivity import FlowByActivity


def eia_mer_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which
    data is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls = []
    for tbl in config['tbls']:
        url = build_url.replace("__tbl__", tbl)
        urls.append(url)
    return urls


def eia_mer_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: response, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as fp:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    df['Tbl'] = resp.url.split("tbl=",1)[1]
    return df


def parse_tables(desc):
    """
    Based on description field of {Tbl}: {description}
    returns tuple of (FlowName, ActivityProducedBy, ActivityConsumedBy)
    """
    tbl, d = desc.split(':')
    if tbl == 'T01.02':
        # desc = 'T01.02: Biomass Energy Production'
        flow = d.split('Production')[0].strip()
        flow = flow.replace(' (', ', ').rstrip(')')
        # ^^ remove parenthesis and replace with comma for consistency
        return (flow, d.strip(), None)
    elif tbl == 'T01.03':
        # desc = 'T01.03: Petroleum Consumption (Excluding Biofuels)'
        flow = d.replace(' Consumption' ,'').strip()
        return (flow, None, d.strip())
    elif tbl == 'T01.04A':
        # desc = 'T01.04A: Petroleum Products, Excluding Biofuels, Imports'
        flow = d.split('Imports')[0].strip().rstrip(',')
        return (flow, None, d.strip())
    elif tbl == 'T01.04B':
        # desc = 'T01.04B: Coal Coke Exports'
        flow = d.split('Exports')[0].strip().rstrip(',')
        return (flow, d.strip(), None)
    elif tbl == 'T02.02':
        # desc = 'T02.02: Total Energy Consumed by the Residential Sector'
        flow = d.split('Consumed')[0].strip()
        return (flow, None, 'Residential Sector')
    elif tbl in ('TA2', 'TA4', 'TA5'):
        # desc = 'TA2: Crude Oil Production Heat Content'
        sec = d.split('Heat Content')[0].strip()
        flow = re.split('Production|Imports|Exports|Consumption', sec)[0].strip()
        # ^^ extract just the fuel name
        if ',' in sec:
            flow = f'{flow}, {sec.split(", ")[1].strip()}'
        ## ^^ if there is a comma, grab the item after and append to the flow
        return (flow, sec, None)
    else:
        return (None, None, None)


def eia_mer_parse(*, df_list, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)

    ## get month = 13 for annual total
    df = (df
          .assign(YYYYMM = lambda x: x['YYYYMM'].astype(str))
          .query('YYYYMM.str.endswith("13")')
          .reset_index(drop=True)
          .assign(detail = lambda x: x['Tbl'] + ': ' + x['Description'])
          .assign(Description = lambda x: x['Tbl'])
          )
    data = df.detail.apply(parse_tables)

    df['Value'] = df['Value'].replace('Not Available', 0)
    df['Value'] = df['Value'].replace('Not Applicable', 0)
    df = (df
          .assign(Year = lambda x: x['YYYYMM'].str[:4])
          .assign(FlowName = pd.Series(y[0] for y in data))
          .assign(ActivityProducedBy = pd.Series(y[1] for y in data))
          .assign(ActivityConsumedBy = pd.Series(y[2] for y in data))
          .assign(FlowAmount = lambda x: x['Value'].astype(float))
          .assign(Unit = lambda x: x['Unit'].str.replace(' per ', ' / '))
          .drop(columns=['Tbl', 'Value', 'Column_Order', 'YYYYMM'])
          )

    df = assign_fips_location_system(df, 2024)
    # hard code data
    df['Class'] = 'Energy'
    df['SourceName'] = 'EIA_MER'
    df['Location'] = '00000'

    df['FlowType'] = np.where(df['Description'].isin(['T01.02']),
                              'ELEMENTARY_FLOW',
                              'TECHNOSPHERE_FLOW')

    # Fill in the rest of the Flow by fields so they show
    # "None" instead of nan.
    df['Compartment'] = 'None'
    df['MeasureofSpread'] = 'None'
    df['DistributionType'] = 'None'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def invert_heat_content(
        fba: FlowByActivity, **_
    ) -> FlowByActivity:
    """Convert units for the heat content values to align with those in the fba,
    invert these values to use attribution_method: multiplication instead of
    attribution_method: division. clean_fba_w_sec fxn"""
    units = fba.Unit.str.split('/')
    # convert million btu to quadrilliion btu and invert
    fba['FlowAmount'] = np.where(fba['Unit'].str.startswith('Million Btu'),
                                 (1 / fba['FlowAmount']) * 1e9,
                                 fba['FlowAmount'])
    # convert btu to quadrilliion btu and invert
    fba['FlowAmount'] = np.where(fba['Unit'].str.startswith('Btu'),
                                 (1 / fba['FlowAmount']) * 1e15,
                                 fba['FlowAmount'])

    fba['Unit'] = pd.Series(f'{y[1].strip()}/{y[0].strip()}' for y in units)

    fba['Unit'] = np.where(fba['Unit'].str.endswith('Million Btu'),
                           fba['Unit'].str.replace('Million Btu',
                                                   'Quadrillion Btu'),
                           fba['Unit'])
    fba['Unit'] = np.where(fba['Unit'].str.endswith('/Btu'),
                           fba['Unit'].str.replace('/Btu',
                                                   '/Quadrillion Btu'),
                           fba['Unit'])
    return fba


def map_energy_flows(
        fba: FlowByActivity, **_
    ) -> FlowByActivity:
    """Maps energy flows to the FEDEFL after attribution.
    clean_fba_after_attribution fxn"""
    fba = apply_flow_mapping(fba, 'EIA_MER', 'ELEMENTARY_FLOW',
                             ignore_source_name=True)
    return fba

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='EIA_MER', year='2012-2023')
    fba = flowsa.getFlowByActivity('EIA_MER', 2020)
