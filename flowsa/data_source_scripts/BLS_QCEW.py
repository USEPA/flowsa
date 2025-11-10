# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau
of Labor Statistics. Writes out to various FlowBySector class files for
these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import zipfile
import io
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowsa_log import log
from flowsa.naics import industry_spec_key, return_max_sector_level


def BLS_QCEW_URL_helper(*, build_url, year, **_):
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
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    urls = []

    url = build_url
    url = url.replace('__year__', str(year))
    urls.append(url)

    return urls


def bls_qcew_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # initiate dataframes list
    df_list = []
    # unzip folder that contains bls data in ~4000 csv files
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # Only want state info
            if "singlefile" in name:
                data = f.open(name)
                df_state = pd.read_csv(data, header=0, dtype=str)
                df_list.append(df_state)
                # concat data into single dataframe
                df = pd.concat(df_list, sort=False)
                df = df[['area_fips', 'own_code', 'industry_code', 'year',
                         'annual_avg_estabs', 'annual_avg_emplvl',
                         'total_annual_wages']]
        return df


def bls_qcew_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, sort=False)
    # drop rows don't need
    df = df[~df['area_fips'].str.contains(
        'C|USCMS|USMSA|USNMS')].reset_index(drop=True)
    df.loc[df['area_fips'] == 'US000', 'area_fips'] = US_FIPS
    # set datatypes
    float_cols = [col for col in df.columns if col not in
                  ['area_fips', 'own_code', 'industry_code', 'year']]
    for col in float_cols:
        df[col] = df[col].astype('float')
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin(['1', '2', '3', '5'])]
    # replace ownership code with text defined by bls
    # https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
    replace_dict = {'1': 'Federal Government',
                    '2': 'State Government',
                    '3': 'Local Government',
                    '5': 'Private'}
    for key in replace_dict.keys():
        df['own_code'] = df['own_code'].replace(key, replace_dict[key])
    # Rename fields
    df = df.rename(columns={'area_fips': 'Location',
                            'industry_code': 'ActivityProducedBy',
                            'year': 'Year',
                            'annual_avg_emplvl': 'Number of employees',
                            'annual_avg_estabs': 'Number of establishments',
                            'total_annual_wages': 'Annual payroll'})
    # Reformat FIPs to 5-digit
    df['Location'] = df['Location'].apply('{:0>5}'.format)
    # use "melt" fxn to convert colummns into rows
    df2 = df.melt(id_vars=["Location", "ActivityProducedBy", "Year",
                          'own_code'],
                  var_name="FlowName",
                  value_name="FlowAmount")
    # specify unit based on flowname
    df2['Unit'] = np.where(df2["FlowName"] == 'Annual payroll', "USD", "p")
    # specify class
    df2.loc[df2['FlowName'] == 'Number of employees', 'Class'] = 'Employment'
    df2.loc[df2['FlowName'] == 'Number of establishments', 'Class'] = 'Other'
    df2.loc[df2['FlowName'] == 'Annual payroll', 'Class'] = 'Money'
    # update flow name
    df2['FlowName'] = df2['FlowName'] + ', ' + df2['own_code']
    df2 = df2.drop(columns='own_code')
    # add location system based on year of data
    df2 = assign_fips_location_system(df2, year)
    # add hard code data
    df2['SourceName'] = 'BLS_QCEW'
    # Add tmp DQ scores
    df2['DataReliability'] = 5
    df2['DataCollection'] = 5
    df2['Compartment'] = None
    df2['FlowType'] = "ELEMENTARY_FLOW"

    return df2


def clean_qcew(fba: FlowByActivity, **kwargs):
    #todo: check function method for state
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')

    totals = (
        fba
        .query('ActivityProducedBy.str.len() == 3')
        [['Location', 'ActivityProducedBy', 'FlowAmount']]
        .assign(ActivityProducedBy=lambda x: (x.ActivityProducedBy
                                              .str.slice(stop=2)))
        .groupby(['Location', 'ActivityProducedBy']).agg('sum')
        .reset_index()
        .rename(columns={'FlowAmount': 'new_total'})
    )

    merged = fba.merge(totals, how='left')

    fixed = (
        merged
        .assign(FlowAmount=merged.FlowAmount.mask(
            (merged.ActivityProducedBy.str.len() == 2)
            & (merged.FlowAmount == 0),
            merged.new_total
        ))
        .drop(columns='new_total')
        .reset_index(drop=True)
    )

    target_naics = set(
        industry_spec_key(fba.config['industry_spec'],
                          fba.config['target_naics_year'])
        .target_naics)
    filtered = (
        fixed
        .assign(ActivityProducedBy=fixed.ActivityProducedBy.mask(
            (fixed.ActivityProducedBy + '0').isin(target_naics),
            fixed.ActivityProducedBy + '0'
        ))
        .query('ActivityProducedBy in @target_naics')
    )

    return filtered


def clean_qcew_for_fbs(fba: FlowByActivity, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    fba['Flowable'] = 'Jobs'
    return fba


def estimate_suppressed_qcew(fba: FlowByActivity) -> FlowByActivity:
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')
    else:
        log.critical('At a subnational scale, this might take a long time.')

    fba2 = (fba
            .assign(Unattributed=fba.FlowAmount.copy(),
                    Attributed=0)
            .assign(descendants='')
            .replace({'ActivityProducedBy': {'31-33': '3X',
                                             '44-45': '4X',
                                             '48-49': '4Y'}})
            )

    # subset the bls data to only keep parent-child sectors up to the target sector level,
    # does not drop non-naics because we want to keep all data, as some suppressed
    # data will be attributed to these non-naics

    # determine max sector length for estimating suppressed data
    max_level = return_max_sector_level(fba.config['industry_spec'])

    # subset BLS data to drop all child naics after target sector level
    fba3 = (
        fba2
        .query(f'ActivityProducedBy.str.len() <= {max_level}')
        .reset_index(drop=True)
    )

    for level in range(max_level-1, 1, -1):
        log.info(f"Identifying sector descendants for NAICS {level}")
        descendants = pd.DataFrame(
            fba3
            .drop(columns='descendants')
            .query(f'ActivityProducedBy.str.len() > {level}')
            .assign(
                parent=lambda x: x.ActivityProducedBy.str.slice(stop=level)
            )
            # replace parent values if parent is a range
            .replace({'parent': {'31': '3X',
                                 '32': '3X',
                                 '33': '3X',
                                 '44': '4X',
                                 '45': '4X',
                                 '48': '4Y',
                                 '49': '4Y'
                                 }})
            .groupby(['FlowName', 'Location', 'parent'])
            .agg({'Unattributed': 'sum', 'ActivityProducedBy': ' '.join})
            .reset_index()
            .rename(columns={'ActivityProducedBy': 'descendants',
                             'Unattributed': 'descendant_flows',
                             'parent': 'ActivityProducedBy'})
        )

        fba3 = (
            fba3
            .merge(descendants,
                   how='left',
                   on=['FlowName', 'Location', 'ActivityProducedBy'],
                   suffixes=(None, '_y'))
            .fillna({'descendant_flows': 0, 'descendants_y': ''})
            .assign(
                descendants=lambda x: x.descendants.mask(x.descendants == '',
                                                         x.descendants_y),
                Unattributed=lambda x: (x.Unattributed -
                                      x.descendant_flows).mask(
                    x.Unattributed - x.descendant_flows < 0, 0),
                Attributed=lambda x: (x.Attributed +
                                      x.descendant_flows)
            )
            .drop(columns=['descendant_flows', 'descendants_y'])
        )
    fba3 = fba3.drop(columns=['descendants'])

    indexed = (
        fba3
        .assign(n2=fba3.ActivityProducedBy.str.slice(stop=2),
                n3=fba3.ActivityProducedBy.str.slice(stop=3),
                n4=fba3.ActivityProducedBy.str.slice(stop=4),
                n5=fba3.ActivityProducedBy.str.slice(stop=5),
                n6=fba3.ActivityProducedBy.str.slice(stop=6),
                location=fba3.Location,
                category=fba3.FlowName)
        .replace({'FlowAmount': {0: np.nan},
                  'n2': {'31': '3X', '32': '3X', '33': '3X',
                         '44': '4X', '45': '4X',
                         '48': '4Y', '49': '4Y'}})
        .set_index(['n2', 'n3', 'n4', 'n5', 'n6', 'location', 'category'],
                   verify_integrity=True)
    )

    def fill_suppressed(
        flows, level: int, activity
    ):
        parent = flows[flows[activity].str.len() == level]
        children = flows[flows[activity].str.len() == level + 1]
        null_children = children[children['flow_suppressed']]

        if null_children.empty or parent.empty:
            return flows
        else:
            value = max(parent['Unattributed'].iloc[0] / len(null_children), 0)
            # update the null children by adding the unattributed data to
            # the attributed data
            null_children = (
                null_children
                .assign(FlowAmount=value+null_children['Attributed'])
                .assign(Unattributed=value)
            )
            flows.loc[null_children.index, ['FlowAmount', 'Unattributed']] = (
                null_children)[['FlowAmount', 'Unattributed']]

            return flows

    unsuppressed = indexed.copy()
    # replace 0 values with np.nan for suppressed data to be estimated
    unsuppressed['FlowAmount'] = unsuppressed['FlowAmount'].mask(unsuppressed['FlowAmount'] == 0)
    unsuppressed['flow_suppressed'] = unsuppressed['FlowAmount'].isna()
    for level in range(2, max_level, 1):
        log.info(f"Estimating suppressed NAICS {level + 1}")
        groupcols = ["{}{}".format("n", i) for i in range(2, level+1)] + [
            'location', 'category']
        unsuppressed = unsuppressed.groupby(
            level=groupcols).apply(
            fill_suppressed, level, 'ActivityProducedBy')

    aggregated = (
        unsuppressed
        .reset_index(drop=True)
        .fillna({'FlowAmount': 0})
        .drop(columns=['Unattributed', 'Attributed', 'flow_suppressed'])
        .assign(FlowName='Number of employees')
        .replace({'ActivityProducedBy': {'3X': '31-33',
                                         '4X': '44-45',
                                         '4Y': '48-49'}})
        .aggregate_flowby()
    )

    return aggregated


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='BLS_QCEW', year=2022)
    fba = flowsa.getFlowByActivity('BLS_QCEW', year=2022)
