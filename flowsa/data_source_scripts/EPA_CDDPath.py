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
from flowsa.location import US_FIPS
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
from flowsa.dataclean import standardize_units
from flowsa.schema import flow_by_activity_mapped_fields


# Read pdf into list of DataFrame
def epa_cddpath_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    df1 = (pd.read_excel(resp.content,
                         sheet_name='Final Results',
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

    df2 = (pd.read_excel(resp.content,
                         sheet_name='Final Results',
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
    :param args: dictionary, used to run flowbyactivity.py
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


def combine_cdd_path(*, resp, **_):
    """Call function to generate combined dataframe from csv file and
    excel dataset, applying the ActivityProducedBy across the flows.
    """
    file = 'EPA_2016_Table5_CNHWCGenerationbySource_Extracted_' \
           'UsingCNHWCPathNames.csv'
    df_csv = pd.read_csv(externaldatapath + file, header=0,
                         names=['FlowName', 'ActivityProducedBy',
                                'FlowAmount'])
    df_csv['pct'] = (df_csv['FlowAmount']/
                     df_csv.groupby(['FlowName'])
                     .FlowAmount.transform('sum'))
    df_csv = df_csv.drop(columns=['FlowAmount'])
    df_excel = epa_cddpath_call(resp=resp)

    df = df_excel.merge(df_csv, how='left', on='FlowName')
    df['pct'] = df['pct'].fillna(1)
    df['FlowAmount'] = df['FlowAmount'] * df['pct']
    df['ActivityProducedBy'] = df['ActivityProducedBy'].fillna('Buildings')
    df = df.drop(columns=['pct'])
    return df


def assign_wood_to_engineering(fba, **_):
    """clean_fba_df_fxn that reclassifies Wood from 'Other' to
    'Other - Wood' so that its mapping can be adjusted to only use
    237990/Heavy engineering NAICS according to method in Meyer et al. 2020
    :param fba: df, FBA of CDDPath
    :return: df, CDDPath FBA with wood reassigned
    """

    # Update wood to a new activity for improved mapping
    fba.loc[((fba.FlowName == 'Wood') &
           (fba.ActivityProducedBy == 'Other')),
           'ActivityProducedBy'] = 'Other - Wood'

    # if no mapping performed, still update units
    if 'short tons' in fba['Unit'].values:
        fba = standardize_units(fba)

    return fba


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
    # flowsa.flowbyactivity.main(source='EPA_CDDPath', year=2014)
    # fba = flowsa.getFlowByActivity(datasource='EPA_CDDPath', year=2014)

    flowsa.flowbysector.main(method='CDD_concrete_national_2014')
    fbs = flowsa.getFlowBySector(methodname='CDD_concrete_national_2014')
