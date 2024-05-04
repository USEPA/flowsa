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
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system


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
        return (flow, d.strip(), None)
    elif tbl == 'T01.03':
        # desc = 'T01.03: Petroleum Consumption (Excluding Biofuels)'
        flow = d.replace(' Consumption' ,'').strip()
        return (flow, None, d.strip())
    elif tbl == 'T01.04A':
        # desc = 'T01.04A: Petroleum Products, Excluding Biofuels, Imports'
        flow = d.split('Imports')[0].strip().rstrip(',')
        return (flow, None, d.strip())
    elif tbl == 'T02.02':
        # desc = 'T02.02: Total Energy Consumed by the Residential Sector'
        flow = d.split('Consumed')[0].strip()
        return (flow, None, 'Residential Sector')
    else:
        return (None, None, None)


def eia_mer_parse(*, df_list, year, config, **_):
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
          .query(f'YYYYMM == {year}13')
          .reset_index(drop=True)
          .assign(Description = lambda x: x['Tbl'] + ': ' + x['Description'])
          )
    data = df.Description.apply(parse_tables)

    df['Value'] = df['Value'].replace('Not Available', 0)
    df = (df
          .assign(Year = year)
          .assign(FlowName = pd.Series(y[0] for y in data))
          .assign(ActivityProducedBy = pd.Series(y[1] for y in data))
          .assign(ActivityConsumedBy = pd.Series(y[2] for y in data))
          .assign(FlowAmount = lambda x: x['Value'].astype(float))
          .drop(columns=['Tbl', 'Value', 'Column_Order', 'YYYYMM'])
          )

    df = assign_fips_location_system(df, year)
    # hard code data
    df['Class'] = 'Energy'
    df['SourceName'] = 'EIA_MER'
    df['Location'] = '00000'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    # Fill in the rest of the Flow by fields so they show
    # "None" instead of nan.
    df['Compartment'] = 'None'
    df['MeasureofSpread'] = 'None'
    df['DistributionType'] = 'None'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='EIA_MER', year=2020)
    fba = flowsa.getFlowByActivity('EIA_MER', 2020)
