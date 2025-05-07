# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for National Income and Product Accounts from BEA.


"""

import io
import zipfile
import pandas as pd
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def bea_nipa_call(*, resp, config, **_):

    zip_file = zipfile.ZipFile(io.BytesIO(resp.content))
    df_list = []
    for filename in zip_file.namelist():
        if filename in config['files']:
            with zip_file.open(filename) as file:
                df_list.append(
                    pd.read_csv(file)
                    .rename(columns={'%SeriesCode':'SeriesCode',
                                     'TableId:LineNo': 'Table_and_Line'})
                    )

    return df_list

def bea_nipa_parse(*, df_list, source, year, config, **_):
    """
    Parse BEA data for GrossOutput, Make, and Use tables
    :param source:
    :param year:
    :return:
    """
    for df in df_list:
        if 'TableTitle' in df:
            tables = df
        elif 'Value' in df:
            data = df
            data['Value'] = data['Value'].str.replace(",", "").astype(float) * 1000000
        elif 'SeriesLabel' in df:
            series = df

    def extract_series_by_table(table):
        series1 = series.query('Table_and_Line.str.contains(@table)').reset_index(drop=True)
        # Split the strings by '|'
        series1['Table_and_Line'] = series1['Table_and_Line'].str.split('|')
        # Explode the lists into separate rows
        df = series1.explode('Table_and_Line')
        df = df.query('Table_and_Line.str.contains(@table)').reset_index(drop=True)
        df['TableId'] = df['Table_and_Line'].str.split(":", expand=True)[0]
        df['Line'] = df['Table_and_Line'].str.split(":", expand=True)[1].astype('int')
        df = df.drop(columns=['Table_and_Line'])
        df = df.merge(tables, on='TableId', how='left', validate='m:1')
        return df.reset_index(drop=True)

    def generate_data_table(table):
        series = extract_series_by_table(table)
        series1_wide = (series
                        .merge(data.query('Period > 2011'),
                                  how='left', on='SeriesCode')
                        # .pivot_table(index=[c for c in series.columns if c not in ['Period', 'Value']],
                        #          columns='Period', values='Value', aggfunc='mean')
                        #         # use 'mean' in case of errors in duplicates
                        # .reset_index()
                        .sort_values(by='Line')
                        )
        return series1_wide

    df = pd.DataFrame()
    df = pd.concat([generate_data_table(c) for c in config['tables']],
                   ignore_index=True)
    df = df.drop(columns=['SeriesCodeParents', 'DefaultScale', 'CalculationType', 'MetricName'])

    df = (df
          .assign(Description = lambda x: x['TableId'] + ": " + x['SeriesCode']
                  + " - " + x['Line'].astype(str))
          .assign(Year = lambda x: x['Period'].astype('Int64').astype(str))
          .rename(columns={'SeriesLabel': 'ActivityProducedBy',
                           'Value': 'FlowAmount'})
          .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy']
                  + ' (' + x['Line'].astype(str) + ')')
          )

    # columns relevant to all BEA data
    df["SourceName"] = source
    df['FlowName'] = "USD"
    df['ActivityConsumedBy'] = '' # set something here?
    df['Compartment'] = '' # set something here?
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df["Location"] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def extract_table_info(fba, **_):
    """
    """
    # extract table info for easier parsing
    fba[['Table', 'Code_Line']] = fba['Description'].str.split(': ', expand=True)
    fba[['Code', 'Line']] = fba['Code_Line'].str.split(' - ', expand=True)
    fba = (fba
           .assign(Line = lambda x: x['Line'].astype(int))
           .drop(columns=['Code_Line'])
           # .sort_values(by=['Table', 'Line'])
           )
    return fba


def drop_unassigned(fba, **_):
    """clean_fba_w_sec fxn
    """
    # Because ACB is assigned in the method yaml, need to drop those that don't
    # have an original APB assignment in the mapping file
    fba = fba[~fba['SectorProducedBy'].isna()]

    return fba


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='BEA_NIPA', year='2022-2024')
    fba = pd.DataFrame()
    for y in range(2022, 2025):
        fba = pd.concat([fba, flowsa.getFlowByActivity('BEA_NIPA', y)],
                        ignore_index=True)

    # extract table info for easier parsing
    fba[['Table', 'Code_Line']] = fba['Description'].str.split(': ', expand=True)
    fba[['Code', 'Line']] = fba['Code_Line'].str.split(' - ', expand=True)
    fba = (fba
           .assign(Line = lambda x: x['Line'].astype(int))
           .drop(columns=['Code_Line'])
           .sort_values(by=['Table', 'Line'])
           )
