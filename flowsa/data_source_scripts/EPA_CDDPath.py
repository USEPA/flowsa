# EPA_CDDPath.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Construction and Demolition Debris 2014 Final Disposition Estimates
Using the CDDPath Method v2
https://edg.epa.gov/metadata/catalog/search/resource/details.page?
uuid=https://doi.org/10.23719/1503167
Last updated: 2018-11-07
"""

import pandas as pd
from tabula.io import read_pdf
import re
import os
from esupy.remote import headers

import flowsa.flowbyactivity
from flowsa.location import US_FIPS
from flowsa.flowsa_log import log
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
from flowsa.dataclean import standardize_units
from flowsa.schema import flow_by_activity_mapped_fields


def call_cddpath_model(*, resp, year, config, **_):
    """
    Convert response for calling url to dataframe of CDDPath model,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :param year:
    :param config: 
    :return: pandas dataframe of original source data
    """
    # if year requires local file, bypass url call to replace with file in
    # external data
    if year == '2014':
        source_data = resp.content
        sheet_name = 'Final Results'
    else:
        try:
            file = config['file'][year]
        except KeyError:
            log.error('CDDPath filepath not provided in FBA method')
            raise
        source_data = externaldatapath / file
        if os.path.isfile(source_data):
            log.info(f"Reading from local file {file}")
        else:
            log.error(f"{file} not found in external data directory. "
                      "The source dataset is not available publicly, but "
                      "the published FBA can be found on Data Commons at "
                      "https://dmap-data-commons-ord.s3.amazonaws.com/index.html?prefix=flowsa/")
            raise FileNotFoundError
        sheet_name = f"Final Results {year}"

    # Convert response to dataframe
    df1 = (pd.read_excel(source_data,
                         sheet_name=sheet_name,
                         # exclude extraneous rows & cols
                         header=2, nrows=30, usecols="A, B",
                         # give columns tidy names
                         names=["FlowName", "Landfill"],
                         # specify data types
                         dtype={'a': str, 'b': float})
           .dropna()  # drop NaN's produced by Excel cell merges
           .melt(id_vars=["FlowName"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount"))

    df2 = (pd.read_excel(source_data,
                         sheet_name=sheet_name,
                         # exclude extraneous rows & cols
                         header=2, nrows=30, usecols="A, C, D",
                         # give columns tidy names
                         names=["FlowName", "ActivityConsumedBy", "FlowAmount"],
                         # specify data types
                         dtype={'a': str, 'c': str, 'd': float})
           .fillna(method='ffill'))
    df = pd.concat([df1, df2], ignore_index=True)

    return df


def epa_cddpath_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)

    # hardcode
    df['Class'] = 'Other'  # confirm this
    df['SourceName'] = 'EPA_CDDPath'  # confirm this
    df['Unit'] = 'short tons'
    df['FlowType'] = 'WASTE_FLOW'
    # df['Compartment'] = 'waste'  # confirm this
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = year
    # df['MeasureofSpread'] = "NA"  # none available
    df['DataReliability'] = 5  # confirm this
    df['DataCollection'] = 5  # confirm this

    return df


def combine_cdd_path(*, resp, year, config, **_):
    """Call function to generate combined dataframe from generation
    by source dataset and excel CDDPath model,
    applying the ActivityProducedBy across the flows.
    """
    # Extract generation by source data from link or file in externaldatapath
    df_csv = None
    file = config['generation_by_source'].get(year)
    if type(file) == dict:
        df_csv = call_generation_by_source(file)
    if df_csv is None:
        # if not available, default to 2014 ratios
        file = config['generation_by_source'].get('2014')
        df_csv = pd.read_csv(externaldatapath / file, header=0,
                             names=['FlowName', 'ActivityProducedBy',
                                    'FlowAmount'])
    df_csv['pct'] = (df_csv['FlowAmount']/
                     df_csv.groupby(['FlowName'])
                     .FlowAmount.transform('sum'))
    df_csv = df_csv.drop(columns=['FlowAmount'])
    
    # Extract data by end use from CDDPath excel model
    df_excel = call_cddpath_model(resp=resp, year=year, config=config)

    df = df_excel.merge(df_csv, how='left', on='FlowName')
    df['pct'] = df['pct'].fillna(1)
    df['FlowAmount'] = df['FlowAmount'] * df['pct']
    df['ActivityProducedBy'] = df['ActivityProducedBy'].fillna('Buildings')
    df = df.drop(columns=['pct'])
    return df


def call_generation_by_source(file_dict):
    """Extraction generation by source data from pdf"""
    pg = file_dict.get('pg')
    url = file_dict.get('url')
    df = read_pdf(url, pages=pg, stream=True,
              guess=True,
              user_agent=headers.get('User-Agent')
              )[0]
    # set headers
    df = df.rename(columns={df.columns[0]: 'FlowName',
                            df.columns[1]: 'Buildings',
                            df.columns[2]: 'Roads and Bridges',
                            df.columns[3]: 'Other'})
    # drop total row
    df = df[df['FlowName'] != 'Total']
    # drop notes
    df['FlowName'] = df['FlowName'].apply(lambda x: re.sub(r'\d+', '', x))

    # make sure material names match
    name_dict = {"Wood Products": "Wood",
                 "Drywall and Plasters": "Gypsum Drywall",
                 "Steel": "Metal",
                 "Asphalt Concrete": "Reclaimed Asphalt Pavement"}

    df['FlowName'] = df['FlowName'].map(name_dict).fillna(df['FlowName'])

    # melt
    df2 = df.melt(id_vars=["FlowName"],
                  var_name="ActivityProducedBy",
                  value_name="FlowAmount")
    return df2


def keep_activity_consumed_by(fba, **_):
    """clean_allocation_fba"""
    fba['ActivityProducedBy'] = None
    return fba


def cdd_processing(fba, source_dict):
    """clean_fba_df_fxn"""
    material = source_dict.get('cdd_parameter')
    inputs = fba.loc[fba['FlowName'] == material]
    inputs = inputs.reset_index(drop=True)
    outputs = inputs.copy()

    pct_to_mixed = source_dict.get('pct_to_mixed')
    inputs[f"{material} Processing"] = 1-pct_to_mixed
    inputs["Mixed CDD Processing"] = pct_to_mixed

    def melt_and_apply_percentages(df, fbacol='ActivityConsumedBy'):
        df = (df.melt(id_vars = flow_by_activity_mapped_fields,
                      value_vars=[f"{material} Processing", "Mixed CDD Processing"])
              .drop(columns=[fbacol])
              .rename(columns={'variable': fbacol})
              )
        df['FlowAmount'] = df['FlowAmount'] * df['value']
        return df

    inputs = melt_and_apply_percentages(inputs)

    cols = flow_by_activity_mapped_fields.copy()
    cols.pop('FlowAmount')
    inputs = aggregator(inputs, cols)
    inputs['Flowable'] = f"{material} waste"

    landfill_from_mixed = source_dict.get('landfill_from_mixed')
    recycled_from_mixed = source_dict.get('recycled_from_mixed')

    outputs["Mixed CDD Processing"] = recycled_from_mixed
    outputs.loc[outputs["ActivityConsumedBy"] == "Landfill",
                "Mixed CDD Processing"] = landfill_from_mixed
    # Remainder from single material MRF
    outputs[f"{material} Processing"] = 1 - outputs["Mixed CDD Processing"]
    outputs = melt_and_apply_percentages(outputs, "ActivityProducedBy")
    outputs = aggregator(outputs, cols)
    outputs.loc[outputs['ActivityConsumedBy'] == 'Remanufacture',
                'ActivityConsumedBy'] = f'Remanufacture - {material}'
    outputs['Flowable'] = outputs['ActivityConsumedBy']
    outputs['FlowType'] = 'TECHNOSPHERE_FLOW'
    outputs.loc[outputs['Flowable'] == 'Landfill',
                ['Flowable', 'FlowType']] = [f"{material} waste", 'WASTE_FLOW']

    df1 = pd.concat([inputs, outputs], ignore_index=True)

    return df1


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='EPA_CDDPath', year=2018)
    fba = flowsa.flowbyactivity.getFlowByActivity(datasource='EPA_CDDPath', year=2018)

    # fbs = flowsa.return_FBS(methodname='CDD_concrete_national_2014')
