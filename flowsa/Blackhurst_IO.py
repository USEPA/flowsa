# write_FBA_Blackhurst_IO.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Scrapes data from Blackhurst paper 'Direct and Indirect Water Withdrawals for US Industrial Sectors' (Supplemental info)
"""

import tabula
import io
from flowsa.common import *
from flowsa.flowbyfunctions import  assign_fips_location_system


# Read pdf into list of DataFrame
def bh_call(url, bh_response, args):

    pages = range(5, 13)
    bh_df_list = []
    for x in pages:
        bh_df = tabula.read_pdf(io.BytesIO(bh_response.content), pages=x, stream=True)[0]
        bh_df_list.append(bh_df)

    bh_df = pd.concat(bh_df_list, sort=False)

    return bh_df


def bh_parse(dataframe_list, args):
    # concat list of dataframes (info on each page)
    df = pd.concat(dataframe_list, sort=False)
    df = df.rename(columns={"I-O code": "ActivityConsumedBy",
                            "I-O description": "Description",
                            "gal/$M": "FlowAmount",
                            })
    # hardcode
    df['Unit'] = 'gal/$M'
    df['SourceName'] = 'Blackhurst_IO'
    df['Class'] = 'Water'
    df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2002')
    df['Year'] = '2002'

    return df


def convert_blackhurst_data_to_gal_per_year(df, attr):

    import flowsa
    from flowsa.mapping import add_sectors_to_flowbyactivity
    from flowsa.flowbyfunctions import clean_df, fba_fill_na_dict

    # test
    #df = fba_allocation.copy()

    # load the bea make table
    bmt = flowsa.getFlowByActivity(flowclass=['Money'],
                                   datasource='BEA_Make_Table',
                                   years=[2002])
    # clean df
    bmt = clean_df(bmt, flow_by_activity_fields, fba_fill_na_dict)
    # drop rows with flowamount = 0
    bmt = bmt[bmt['FlowAmount'] != 0]

    bh_df_revised = pd.merge(df, bmt[['FlowAmount', 'ActivityProducedBy', 'Location']],
                             left_on=['ActivityConsumedBy', 'Location'], right_on=['ActivityProducedBy', 'Location'])

    bh_df_revised.loc[:, 'FlowAmount'] = ((bh_df_revised['FlowAmount_x']/1000000) * (bh_df_revised['FlowAmount_y']))
    bh_df_revised.loc[:, 'Unit'] = 'gal'
    # drop columns
    bh_df_revised = bh_df_revised.drop(columns=["FlowAmount_x", "FlowAmount_y", 'ActivityProducedBy_y'])
    bh_df_revised = bh_df_revised.rename(columns={"ActivityProducedBy_x": "ActivityProducedBy"})

    return bh_df_revised


def convert_blackhurst_data_to_gal_per_employee(df_wsec, attr, method):

    import flowsa
    from flowsa.mapping import add_sectors_to_flowbyactivity
    from flowsa.flowbyfunctions import clean_df, fba_fill_na_dict, agg_by_geoscale, fba_default_grouping_fields, \
        sector_ratios, proportional_allocation_by_location_and_sector, filter_by_geoscale
    from flowsa.BLS_QCEW import clean_bls_qcew_fba

    # test
    #df_wsec = fba_allocation_subset.copy()

    bls = flowsa.getFlowByActivity(flowclass=['Employment'], datasource='BLS_QCEW', years=[2002])
    # clean df
    bls = clean_df(bls, flow_by_activity_fields, fba_fill_na_dict)
    bls = clean_bls_qcew_fba(bls, attr)

    # bls_agg = agg_by_geoscale(bls, 'state', 'national', fba_default_grouping_fields)
    bls_agg = filter_by_geoscale(bls, 'national')

    # assign naics to allocation dataset
    bls_wsec = add_sectors_to_flowbyactivity(bls_agg, sectorsourcename= method['target_sector_source'],
                                             levelofSectoragg='national')
    # drop rows where sector = None ( does not occur with mining)
    bls_wsec = bls_wsec[~bls_wsec['SectorProducedBy'].isnull()]
    bls_wsec = bls_wsec.rename(columns={'SectorProducedBy': 'Sector'})
    #bls_wsec.loc[:, 'sec_tmp'] = bls_wsec['SectorProducedBy'].apply(lambda x: x[0:2])

    # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
    # aren't in list
    sector_list = df_wsec['Sector'].unique().tolist()
    # subset fba allocation table to the values in the activity list, based on overlapping sectors
    bls_wsec = bls_wsec.loc[bls_wsec['Sector'].isin(sector_list)]
    # calculate proportional ratios
    # todo: address problem that occurs here, employee values don't sum to one
    bls_wsec = proportional_allocation_by_location_and_sector(bls_wsec, 'Sector')
    bls_wsec = bls_wsec.rename(columns = {'FlowAmountRatio': 'EmployeeRatio',
                                          'FlowAmount': 'Employees'})

    # merge the two dfs
    df = pd.merge(df_wsec, bls_wsec[['Sector', 'EmployeeRatio', 'Employees']], how='left',
                  left_on='Sector', right_on='Sector')
    df['EmployeeRatio'] = df['EmployeeRatio'].fillna(0)
    # calculate gal/employee in 2002
    df.loc[:, 'FlowAmount'] = (df['FlowAmount'] * df['EmployeeRatio'])/df['Employees']
    df.loc[:, 'Unit'] = 'gal/employee'

    # drop cols
    df = df.drop(columns = ['Employees', 'EmployeeRatio'])

    #bls_wsec = sector_ratios(bls_wsec, 'SectorProducedBy')

    return df
