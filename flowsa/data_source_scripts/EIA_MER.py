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


def eia_mer_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: response, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as fp:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    return df


def decide_flow_name(desc):
    """
    Based on the provided description, determine the FlowName.
    :param desc: str, row description
    :return: str, flowname for row
    """
    if 'Production' in desc:
        return 'Production'
    if 'Consumed' in desc:
        return 'Consumed'
    if 'Sales' in desc:
        return 'Sales'
    if 'Losses' in desc:
        return 'Losses'
    return 'None'


def decide_produced(desc):
    """
    Based on the provided description, determine the ActivityProducedBy.
    :param desc: str, description for row
    :return: str, ActivityProducedBy cell value
    """
    if 'Production' in desc:
        return desc.split('Production')[0].strip()
    return 'None'


def decide_consumed(desc):
    """
    Based on the provided description, determine the ActivityConsumedBy.
    :param desc: str, description cell
    :return: str, ActivityConsumedBy value
    """
    if 'Consumed' in desc:
        return desc.split('Consumed')[0].strip()
    if 'Sales' in desc:
        return desc.split('Sales')[0].strip()
    if 'Losses' in desc:
        return desc.split('Losses')[0].strip()
    return 'None'


def eia_mer_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # Filter only the rows we want, YYYYMM field beginning with 201,
    # for 2010's.
    # For doing year-by-year based on args['year']
    min_year = int(year + '00')
    max_year = int(str(int(year) + 1) + '00')
    df = df[df['YYYYMM'] > min_year]
    df = df[df['YYYYMM'] < max_year]

    output = pd.DataFrame()
    sums_key_map = {}
    sums = []
    for index, row in df.iterrows():
        # Parse out the year value from the YYYYMM field.
        year = str(row['YYYYMM'])[:4]
        name = row['MSN']
        key = (name, year)

        if not sums_key_map.get(key, False):
            flow_name = decide_flow_name(row['Description'])
            act_prod_by = decide_produced(row['Description'])
            act_cons_by = decide_consumed(row['Description'])
            temp_row = {'Description': row['Description'], 'Unit': row['Unit'],
                        'FlowName': flow_name,
                        'ActivityProducedBy': act_prod_by,
                        'ActivityConsumedBy': act_cons_by,
                        'FlowAmount': 0, 'FlowType': 'None', 'Year': year}
            sums.append(temp_row)
            sums_key_map[key] = len(sums) - 1

        try:
            index = sums_key_map[key]
            sums[index]['FlowAmount'] += float(row['Value'])
        except ValueError:
            pass

    output = pd.DataFrame(sums)

    output = assign_fips_location_system(output, year)

    # hard code data
    output['Class'] = 'Energy'
    output['SourceName'] = 'EIA_MER'
    output['Location'] = '00000'
    # Fill in the rest of the Flow by fields so they show
    # "None" instead of nan.
    output['Compartment'] = 'None'
    output['MeasureofSpread'] = 'None'
    output['DistributionType'] = 'None'
    # Add DQ scores
    output['DataReliability'] = 5  # tmp
    output['DataCollection'] = 5  # tmp
    # sort df
    output = output.sort_values(['Location', 'FlowName'])
    # reset index
    output.reset_index(drop=True, inplace=True)
    return output
