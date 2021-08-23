# get_flows_by_activity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
See source_catalog.yaml for available FlowByActivity datasets and
available parameters for getFlowByActivity().
Examples of use of flowsa. Read parquet files as dataframes.
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str, a 'Class' of the flow. Optional. E.g. 'Water'
    :param geographic_level: str, a geographic level of the data.
    Optional. E.g. 'national', 'state', 'county'.
    :return: a pandas DataFrame in FlowByActivity format
"""

import flowsa
from flowsa.common import fbaoutputpath

# EXAMPLE 1
# Load all information for USDA Cropland
usda_cropland_fba_2017 = flowsa.getFlowByActivity(datasource="USDA_CoA_Cropland", year=2017)


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
usgs_water_fba_2015.Location =\
    usgs_water_fba_2015.Location.apply('="{}"'.format)  # maintain leading 0s in location col
usgs_water_fba_2015.to_csv(fbaoutputpath + ds + "_" + str(year_fba) + ".csv", index=False)


# EXAMPLE 3
# loop through and read all USGS_MYB parquets for 2015
# list of minerals
minerals = ['Asbestos', 'Barite', 'Bauxite', 'Beryllium', 'Boron', 'Chromium', 'Clay', 'Cobalt', 'Copper', 'Diatomite',
            'Feldspar', 'Fluorspar', 'Gallium', 'Garnet', 'Gold', 'Graphite', 'Gypsum', 'Iodine', 'IronOre', 'Kyanite',
            'Lead', 'Lime', 'Lithium', 'Magnesium', 'Manganese', 'ManufacturedAbrasive', 'Mica', 'Molybdenum', 'Nickel',
            'Niobium', 'Peat', 'Perlite', 'Phosphate', 'Platinum', 'Potash', 'Pumice', 'Rhenium', 'Salt',
            'SandGravelConstruction', 'SandGravelIndustrial', 'Silver', 'SodaAsh', 'StoneCrushed', 'StoneDimension',
            'Strontium', 'Talc', 'Titanium', 'Tungsten', 'Vermiculite', 'Zeolites', 'Zinc', 'Zirconium']
for m in minerals:
    try:
        datasource_name = 'USGS_MYB_' + m
        df_name = m + '_fba'
        vars()[df_name] = flowsa.getFlowByActivity(datasource_name, year=2015)
    except:
        print('Skipping ' + m)
        pass
