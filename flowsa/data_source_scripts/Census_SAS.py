# Census_SAS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
U.S. Census Service Annual Survey
"""
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS


def census_sas_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    for sheet, name in config['sheets'].items():
        df = (pd.read_excel(resp.content,
                            sheet_name=sheet,
                            header=4)
              .assign(sheet=f'{sheet}: {name}')
              )
        df_list.append(df)

    return df_list


def census_sas_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)
    value_vars = [c for c in df.columns if "Estimate" in c or
                  "Coefficient of Variation" in c]
    id_vars = [c for c in df.columns if c not in value_vars]
    df = (df.dropna(subset=['Item'])
            .melt(id_vars = id_vars,
                  value_vars = value_vars)
            .assign(Year = lambda x: x['variable'].str[0:4])
            .assign(var = lambda x: x['variable'].str[5:])
            .drop(columns='variable')
            .pivot_table(columns=['var'],
                                  index=id_vars + ['Year'],
                                  values='value', aggfunc='sum')
            .reset_index()
            .query('`Tax Status` == "All Establishments"')
            .rename(columns={'NAICS': 'ActivityConsumedBy',
                             'Item': 'FlowName',
                             'sheet': 'Description',
                             'Coefficient of Variation': 'Spread',
                             'Estimate': 'FlowAmount'})
            .drop(columns=['Employer Status', 'Tax Status', 'NAICS Description'])
            )


    # set suppressed values to 0 but mark as suppressed
    # otherwise set non-numeric to nan
    df = (df.assign(
            Suppressed = np.where(df.FlowAmount.str.strip().isin(["S", "Z", "D"]),
                                  df.FlowAmount.str.strip(),
                                  np.nan),
            FlowAmount = np.where(df.FlowAmount.str.strip().isin(["S", "Z", "D"]),
                                  0,
                                  df.FlowAmount)))
    df = (df.assign(
            Suppressed = np.where(df.FlowAmount.str.endswith('(s)') == True,
                                  '(s)',
                                  df.Suppressed),
            FlowAmount = np.where(df.FlowAmount.str.endswith('(s)') == True,
                                  df.FlowAmount.str.replace(',','').str[:-3],
                                  df.FlowAmount),
        ))

    df['Class'] = 'Money'
    df['SourceName'] = 'Census_SAS'
    # millions of dollars
    df['FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    df['Spread'] = pd.to_numeric(df['Spread'], errors='coerce')
    df['MeasureofSpread'] = 'Coefficient of Variation'
    df['Unit'] = 'USD'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Compartment'] = None
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_SAS', year='2013-2022')
    fba = flowsa.getFlowByActivity('Census_SAS', 2022)
