# get_flows_by_activity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
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

# single flowclass, year, datasource, geographic_level default = 'all', file_location default = 'local'
usda_cropland_fba_2017 = flowsa.getFlowByActivity(datasource="USDA_CoA_Cropland", year=2017, flowclass='Land')

# only load state level data and save as csv
# set parameters
fc = 'Water'
year_fba = 2015
ds = "USGS_NWIS_WU"
geo_level_fba = 'state'
# load FBA
usgs_water_fba_2015 = flowsa.getFlowByActivity(datasource=ds, year=year_fba, flowclass=fc,
                                               geographic_level=geo_level_fba).reset_index(drop=True)
# save output to csv
usgs_water_fba_2015.to_csv(fbaoutputpath + ds + "_" + str(year_fba) + ".csv", index=False)