# write_UDSA_ERS_FIWS_xwalk.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk linking the downloaded USDA_CoA_Cropland to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

NAICS8 are unofficial and are not used again after initial aggregation to NAICS6. NAICS8 are based
on NAICS definitions from the Census.

7/8 digit NAICS align with USDA ERS FIWS

"""
import pandas as pd
from flowsa.common import datapath, outputpath

def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names"""
    df = []
    for y in years:
        df = pd.read_parquet(outputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df.append(df)
    df = df[['SourceName', 'ActivityConsumedBy']]
    # rename columns
    df = df.rename(columns={"SourceName": "ActivitySourceName",
                            "ActivityConsumedBy": "Activity"})
    df = df.drop_duplicates()
    return df

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # coa equivalent to agriculture, forestry, fishing, and hunting
    df.loc[df['Activity'] == 'AG LAND', 'Sector'] = '11'

    # coa equivalent to crop production: 111
    df.loc[df['Activity'] == 'AG LAND, CROPLAND', 'Sector'] = '111'

    # coa equivalent to Animal Production and Aquaculture: 112
    df.loc[df['Activity'] == 'AG LAND, PASTURELAND', 'Sector'] = '112'

    ## coa equivalent to soybean farming: 11111
    df.loc[df['Activity'] == 'SOYBEANS', 'Sector'] = '11111'

    # coa aggregates to oilseed (except soybean) farming: 11112
    df.loc[df['Activity'] == 'CANOLA', 'Sector'] = '111120A'
    df.loc[df['Activity'] == 'FLAXSEED', 'Sector'] = '111120B'
    df.loc[df['Activity'] == 'MUSTARD, SEED', 'Sector'] = '111120C'
    df.loc[df['Activity'] == 'RAPESEED', 'Sector'] = '111120D'
    df.loc[df['Activity'] == 'SAFFLOWER', 'Sector'] = '111120E'
    df.loc[df['Activity'] == 'SESAME', 'Sector'] = '111120F'
    df.loc[df['Activity'] == 'SUNFLOWER', 'Sector'] = '111120G'

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
    df.loc[df['Activity'] == 'WHEAT', 'Sector'] = '11114'

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
    # df.loc[df['Activity'] == 'SWEET RICE', 'Sector'] = '' # last year published 2002

    # coa equivalent to vegetable and melon farming: 1112
    df.loc[df['Activity'] == 'VEGETABLE TOTALS', 'Sector'] = '1112'  # this category does include melons

    # coa aggregates to fruit and tree nut farming: 1113
    # in 2017, pineapples included in "orchards" category. Therefore, for 2012, must sum pineapple data to make
    # comparable
    df.loc[df['Activity'] == 'ORCHARDS', 'Sector'] = '111300A'
    df.loc[df['Activity'] == 'PINEAPPLES', 'Sector'] = '111300A1'
    df.loc[df['Activity'] == 'BERRY TOTALS', 'Sector'] = '111300B'

    # coa aggregates to greenhouse nursery and floriculture production: 1114
    df.loc[df['Activity'] == 'HORTICULTURE TOTALS', 'Sector'] = '1114'
    df.loc[df['Activity'] == 'CUT CHRISTMAS TREES', 'Sector'] = '111400A'
    df.loc[df['Activity'] == 'SHORT TERM WOODY CROPS', 'Sector'] = '1114008'

    # coa equivalent to other crop farming: 1119
    df.loc[df['Activity'] == 'CROPS, OTHER', 'Sector'] = '1119'

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
    df.loc[df['Activity'] == 'SUGARBEETS', 'Sector'] = '111991'
    df.loc[df['Activity'] == 'PEANUTS', 'Sector'] = '111992'
    df.loc[df['Activity'] == 'DILL, OIL', 'Sector'] = '111998A'
    df.loc[df['Activity'] == 'GRASSES & LEGUMES TOTALS, SEED', 'Sector'] = '111998B'
    df.loc[df['Activity'] == 'GUAR', 'Sector'] = '111998C'
    df.loc[df['Activity'] == 'HERBS, DRY', 'Sector'] = '111998D'
    df.loc[df['Activity'] == 'HOPS', 'Sector'] = '111998E'
    df.loc[df['Activity'] == 'JOJOBA', 'Sector'] = '111998F'
    df.loc[df['Activity'] == 'MINT, OIL', 'Sector'] = '111998G'
    df.loc[df['Activity'] == 'MISCANTHUS', 'Sector'] = '111998H'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012', '2017']
    # df of unique ers activity names
    df = unique_activity_names('USDA_CoA_Cropland', years)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan' (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = df.sort_values('Sector')
    # reset index
    df.reset_index(drop=True, inplace=True)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_USDA_CoA_Cropland_toNAICS.csv", index=False)
