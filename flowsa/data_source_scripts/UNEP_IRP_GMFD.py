# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Global Material Flows Database
https://unep-irp.fineprint.global/mfa13/export
"""


import io
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowsa_log import log
from flowsa.flowbyactivity import FlowByActivity


def unep_mfa_call(*, resp, url,  **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv('https://unep-irp.fineprint.global/mfa13/export',skiprows=0,
    skipfooter=0)
    return df

def unep_mfa_parse(*, df_list, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    for df in df_list:
        df= df.query("Country == 'United States of America'")
        year_list = [range(2012, 2025)]

        df= df.melt( id_vars=['Country','Category', 'Flow name', 'Flow code', 'Flow unit'], var_name= 'Year', value_name='FlowAmount')
        df = (df
              .query("Year ==  ['2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023','2024']")  
              .rename(columns={'Category':'FlowName','Flow name':'ActivityProducedBy', 'Flow unit':'Unit'})  
              .assign(ActivityProducedBy = lambda x: x['ActivityProducedBy'].str.strip())
              .assign(ActivityProducedBy = lambda x: x['FlowName'] + ', '+ x['ActivityProducedBy'])  
              .drop(columns= ['Country', 'Flow code'])
              .reset_index(drop= True)
              )
        if len(df) == 0:
            log.warning(' Data is missing')

# hard code data
    df['Year'] = df['Year'].astype(str)
    df['SourceName'] = 'UNEP_IRP_GMFD'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['Unit'] = 'MT'
    
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


