# write_Crosswalk_UDSA_ACUP.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded USDA ACUP Fertilizer and Pesticde data to NAICS_12.
Created by selecting unique Activity Names and
manually assigning to NAICS

"""
import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # coa equivalent to soybean farming: 11111
    df.loc[df['Activity'] == 'SOYBEANS', 'Sector'] = '11111'

    # coa aggregates to oilseed (except soybean) farming: 11112
    df.loc[df['Activity'] == 'CANOLA', 'Sector'] = '111120A'
    df.loc[df['Activity'] == 'FLAXSEED', 'Sector'] = '111120B'
    df.loc[df['Activity'] == 'MUSTARD, SEED', 'Sector'] = '111120C'
    df.loc[df['Activity'] == 'RAPESEED', 'Sector'] = '111120D'
    df.loc[df['Activity'] == 'SAFFLOWER', 'Sector'] = '111120E'
    df.loc[df['Activity'] == 'SESAME', 'Sector'] = '111120F'
    df.loc[df['Activity'] == 'SUNFLOWER', 'Sector'] = '111120G'
    df.loc[df['Activity'] == 'CAMELINA', 'Sector'] = '111120H'

    # coa aggregates to dry pea and bean farming: 11113
    df.loc[df['Activity'] == 'BEANS, DRY EDIBLE, (EXCL LIMA), INCL CHICKPEAS', 'Sector'] = '111130A'
    df.loc[df['Activity'] == 'BEANS, DRY EDIBLE, (EXCL CHICKPEAS & LIMA)', 'Sector'] = '111130B'
    df.loc[df['Activity'] == 'BEANS, DRY EDIBLE, LIMA', 'Sector'] = '111130C'
    df.loc[df['Activity'] == 'CHICKPEAS', 'Sector'] = '111130D'
    df.loc[df['Activity'] == 'LENTILS', 'Sector'] = '111130E'
    df.loc[df['Activity'] == 'PEAS, AUSTRIAN WINTER', 'Sector'] = '111130F'
    df.loc[df['Activity'] == 'PEAS, DRY EDIBLE', 'Sector'] = '111130G'
    df.loc[df['Activity'] == 'PEAS, DRY, SOUTHERN (COWPEAS)', 'Sector'] = '111130H'
    # df.loc[df['Activity'] == 'BEANS, MUNG', 'Sector'] = '' # last year published 2002

    # coa equivalent to wheat farming: 11114
    df.loc[df['Activity'] == 'WHEAT, SPRING, (EXCL DURUM)', 'Sector'] = '111140A'
    df.loc[df['Activity'] == 'WHEAT, SPRING, DURUM', 'Sector'] = '111140B'
    df.loc[df['Activity'] == 'WHEAT, WINTER', 'Sector'] = '111140C'

    # coa aggregates to corn farming: 11115
    df.loc[df['Activity'] == 'CORN', 'Sector'] = '11115'
    df.loc[df['Activity'] == 'CORN, GRAIN', 'Sector'] = '111150A'
    df.loc[df['Activity'] == 'CORN, SILAGE', 'Sector'] = '111150B'
    df.loc[df['Activity'] == 'POPCORN, SHELLED', 'Sector'] = '111150C'

    # coa equivalent to rice farming: 11116
    df.loc[df['Activity'] == 'RICE', 'Sector'] = '11116'

    # coa aggregates to all other grain farming: 111199
    df.loc[df['Activity'] == 'BARLEY', 'Sector'] = '111199A'
    df.loc[df['Activity'] == 'BUCKWHEAT', 'Sector'] = '111199B'
    df.loc[df['Activity'] == 'MILLET, PROSO', 'Sector'] = '111199C'
    df.loc[df['Activity'] == 'OATS', 'Sector'] = '111199D'
    df.loc[df['Activity'] == 'RYE', 'Sector'] = '111199E'
    df.loc[df['Activity'] == 'SORGHUM, GRAIN', 'Sector'] = '111199F'
    df.loc[df['Activity'] == 'SORGHUM, SILAGE', 'Sector'] = '111199G'
    df.loc[df['Activity'] == 'SORGHUM, SYRUP', 'Sector'] = '111199H'
    df.loc[df['Activity'] == 'TRITICALE', 'Sector'] = '111199I'
    df.loc[df['Activity'] == 'WILD RICE', 'Sector'] = '111199J'
    df.loc[df['Activity'] == 'EMMER & SPELT', 'Sector'] = '111199K'
    # df.loc[df['Activity'] == 'SWEET RICE', 'Sector'] = '' # last year published 2002

    # coa equivalent to vegetable and melon farming: 1112
    df.loc[df['Activity'] == 'VEGETABLE TOTALS', 'Sector'] = '1112'  # this category includes melons
    df.loc[df['Activity'] == 'TARO', 'Sector'] = '111219'

    # coa aggregates to fruit and tree nut farming: 1113
    # in 2017, pineapples included in "orchards" category.
    # Therefore, for 2012, must sum pineapple data to make comparable
    # orchards associated with 6 naics6, for now, after allocation,
    # divide values associated with these naics by 6
    df.loc[df['Activity'] == 'ORCHARDS', 'Sector'] = '111331'
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland', 'ORCHARDS', 'NAICS_2012_Code', '111332']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName',
                                         'Sector']), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland', 'ORCHARDS', 'NAICS_2012_Code', '111333']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName',
                                         'Sector']), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland', 'ORCHARDS', 'NAICS_2012_Code', '111335']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName',
                                         'Sector']), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland', 'ORCHARDS', 'NAICS_2012_Code', '111336']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName',
                                         'Sector']), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland', 'ORCHARDS', 'NAICS_2012_Code', '111339']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName',
                                         'Sector']), ignore_index=True, sort=True)
    df.loc[df['Activity'] == 'BERRY TOTALS', 'Sector'] = '111334'
    df.loc[df['Activity'] == 'PINEAPPLES', 'Sector'] = '111339'

    # coa aggregates to greenhouse nursery and floriculture production: 1114
    df.loc[df['Activity'] == 'HORTICULTURE TOTALS', 'Sector'] = '1114'
    df.loc[df['Activity'] == 'CUT CHRISTMAS TREES', 'Sector'] = '111421A'
    df.loc[df['Activity'] == 'SHORT TERM WOODY CROPS', 'Sector'] = '111421B'

    # coa equivalent to other crop farming: 1119
    # df.loc[df['Activity'] == 'CROPS, OTHER', 'Sector'] = '1119'
    # df.loc[df['Activity'] == 'FIELD CROPS, OTHER', 'Sector'] = '1119'

    # coa equivalent to tobacco farming: 11191
    df.loc[df['Activity'] == 'TOBACCO', 'Sector'] = '11191'

    # coa aggregates to cotton: 11192
    df.loc[df['Activity'] == 'COTTON', 'Sector'] = '11192'

    # coa aggregates to sugarcane farming: 11193
    df.loc[df['Activity'] == 'SUGARCANE, SUGAR', 'Sector'] = '111930A'
    df.loc[df['Activity'] == 'SUGARCANE, SEED', 'Sector'] = '111930B'

    # coa aggregates to hay farming: 11194
    df.loc[df['Activity'] == 'HAY & HAYLAGE', 'Sector'] = '11194'
    df.loc[df['Activity'] == 'HAY & HAYLAGE (EXCL ALFALFA)', 'Sector'] = '111940A'
    df.loc[df['Activity'] == 'HAY & HAYLAGE, ALFALFA', 'Sector'] = '111940B'
    # df.loc[df['Activity'] == 'HAY', 'Sector'] = '1119401'
    # df.loc[df['Activity'] == 'HAY (EXCL ALFALFA)', 'Sector'] = '1119401A'
    # df.loc[df['Activity'] == 'HAY, ALFALFA', 'Sector'] = '1119401B'
    # df.loc[df['Activity'] == 'HAYLAGE', 'Sector'] = '1119402'
    # df.loc[df['Activity'] == 'HAYLAGE (EXCL ALFALFA)', 'Sector'] = '1119402A'
    # df.loc[df['Activity'] == 'HAYLAGE, ALFALFA', 'Sector'] = '1119402B'

    # coa aggregates to all other crop farming: 11199
    df.loc[df['Activity'] == 'SUGARBEETS', 'Sector'] = '111991A'
    df.loc[df['Activity'] == 'SUGARBEETS, SEED', 'Sector'] = '111991B'
    df.loc[df['Activity'] == 'PEANUTS', 'Sector'] = '111992'
    df.loc[df['Activity'] == 'DILL, OIL', 'Sector'] = '111998A'
    df.loc[df['Activity'] == 'GRASSES & LEGUMES TOTALS, SEED', 'Sector'] = '111998B'
    df.loc[df['Activity'] == 'GUAR', 'Sector'] = '111998C'
    df.loc[df['Activity'] == 'HERBS, DRY', 'Sector'] = '111998D'
    df.loc[df['Activity'] == 'HOPS', 'Sector'] = '111998E'
    df.loc[df['Activity'] == 'JOJOBA', 'Sector'] = '111998F'
    df.loc[df['Activity'] == 'MINT, OIL', 'Sector'] = '111998G'
    # df.loc[df['Activity'] == 'MINT, PEPPERMINT, OIL', 'Sector'] = '111998G1'
    # df.loc[df['Activity'] == 'MINT, SPEARMINT, OIL', 'Sector'] = '111998G2'
    df.loc[df['Activity'] == 'MISCANTHUS', 'Sector'] = '111998H'
    df.loc[df['Activity'] == 'MINT, TEA LEAVES', 'Sector'] = '111998K'
    df.loc[df['Activity'] == 'SWITCHGRASS', 'Sector'] = '111998L'
    df.loc[df['Activity'] == 'FIELD CROPS, OTHER', 'Sector'] = '111998M'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012', '2015', '2017', '2018', '2020']
    # datasource
    datasources = ['USDA_ACUP_Pesticide', 'USDA_ACUP_Fertilizer']
    # loop through datasources:
    for d in datasources:
        # df of unique activity names
        df_list = []
        for y in years:
            dfy = unique_activity_names(d, y)
            df_list.append(dfy)
        df = pd.concat(df_list, ignore_index=True).drop_duplicates()
        # add manual naics 2012 assignments
        df = assign_naics(df)
        # drop any rows where naics12 is 'nan'
        # (because level of detail not needed or to prevent double counting)
        df = df.dropna()
        # assign sector type
        df['SectorType'] = None
        # sort df
        df = order_crosswalk(df)
        # save as csv
        df.to_csv(f"{datapath}activitytosectormapping/NAICS_Crosswalk_{d}.csv", index=False)
