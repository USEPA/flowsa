# UNEP_IRP_GMFD.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Global Material Flows Database
https://unep-irp.fineprint.global/mfa13/export
"""


import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowsa_log import log


def unep_mfa_call(*, resp, config,  **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as f:
        df = pd.read_csv(f)
    return df

def unep_mfa_parse(*, df_list, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list)
    df = df.query("Country == 'United States of America'")
    # year_list = [str(y) for y in range(2012, 2025)]

    df = df.melt(id_vars=['Country','Category', 'Flow name', 'Flow code', 'Flow unit'],
                 var_name= 'Year', value_name='FlowAmount')
    df = (df
          # .query("Year in @year_list")  
          .rename(columns={'Category':'FlowName',
                           'Flow name':'ActivityProducedBy', 
                           'Flow unit':'Unit'})  
          .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy'].str.strip())
          .assign(ActivityProducedBy = lambda x: x['FlowName'] + ', '+ x['ActivityProducedBy'])  
          .drop(columns= ['Country', 'Flow code'])
          .reset_index(drop= True)
          )
    if len(df) == 0:
        log.warning('Data is missing')

    # hard code data
    df['SourceName'] = 'UNEP_IRP_GMFD'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['Unit'] = 'MT'
    df = assign_fips_location_system(df, 2024)
    
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = 'resource'
    df['Class'] = 'Chemicals'
    return df

if __name__ == "__main__":
    import flowsa
    year = '2012-2024'
    flowsa.generateflowbyactivity.main(source='UNEP_IRP_GMFD', year=year)
    fba = pd.DataFrame()
    for y in range(2012, 2025):
        fba = pd.concat([fba, flowsa.getFlowByActivity('UNEP_IRP_GMFD', year=y)],
                        ignore_index=True)
