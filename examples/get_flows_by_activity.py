# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
"""
See source_catalog.yaml for available FlowByActivity datasets and available parameters for getFlowByActivity()
Examples of use of flowsa. Read parquet files as dataframes.
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str, a 'Class' of the flow. Optional. E.g. 'Water'
    :param geographic_level: str, a geographic level of the data. Optional. E.g. 'national', 'state', 'county'.
    :return: a pandas DataFrame in FlowByActivity format
"""

import flowsa
from flowsa.common import fbaoutputpath

# EXAMPLE 1
# Import USDA_CoA_Cropland for 2017, specifying the flowclass and using the geographic_level default = None
usda_cropland_2017_fba = flowsa.getFlowByActivity(datasource="USDA_CoA_Cropland", year=2017, flowclass='Land')


# EXAMPLE 2
# Load state level USGS water use data and save the dataframe as a csv
# set parameters
ds = "USGS_NWIS_WU" # name of the datasource
years_fba = 2015  # year of data to load (can also be 2010 for this example)
fc = 'Water'  # flowclass
geo_level_fba = 'state' # geographic level to load
# load FBA using specified parameters
usgs_water_2015_fba = flowsa.getFlowByActivity(datasource=ds, year=years_fba, flowclass=fc,
                                               geographic_level=geo_level_fba).reset_index(drop=True)
# save output to csv
usgs_water_2015_fba.to_csv(fbaoutputpath + ds + "_" + "_".join(map(str, years_fba)) + ".csv", index=False)


# EXAMPLE 3
# loop through and read all USGS_MYB parquets for 2015
# list of minerals
minerals = ['Barite', 'Beryllium', 'Boron', 'Clay', 'Colbolt', 'Copper', 'Gold', 'Iron_Ore', 'Lead', 'Lime',
            'Magnesium', 'ManufacturedAbrasive', 'Molybdenum', 'Nickel', 'Platinum', 'Rhenium', 'SandGravelCon',
            'SandGravelInd', 'Silver', 'SodaAsh', 'Stone_Crushed', 'Stone_Dimension', 'Titanium', 'Zinc', 'Zirconium']
for m in minerals:
    try:
        datasource_name = 'USGS_MYB_' + m
        df_name = m + '_fba'
        vars()[df_name] = flowsa.getFlowByActivity(datasource_name, year=2015)
    except:
        print('Skipping ' + m)
        pass