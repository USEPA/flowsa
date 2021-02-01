# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
"""

See source_catalog.yaml for available FlowByActivity datasets and available parameters for getFlowByActivity()

Examples of use of flowsa. Read parquet files as dataframes.
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :param geographic_level: default set to 'all', which will load all geographic scales in the FlowByActivity, can
    specify 'national', 'state', 'county'
    :return: a pandas DataFrame in FlowByActivity format
"""

import flowsa
from flowsa.common import fbaoutputpath

# single flowclass, year, datasource, geographic_level default = 'all', file_location default = 'local'
usda_cropland_fba_2017 = flowsa.getFlowByActivity(flowclass=['Land'], years=[2017], datasource="USDA_CoA_Cropland")
# load file from remote server instead of local directory
usda_cropland_fba_2017_remote = flowsa.getFlowByActivity(flowclass=['Land'], years=[2017], datasource="USDA_CoA_Cropland", file_location='remote')

# multiple flowclass
usda_iwms_fba_2013 = flowsa.getFlowByActivity(flowclass=['Land', 'Water'], years=[2013], datasource="USDA_IWMS")

# only load state level data and save as csv
# set parameters
fc = ['Water']
years_fba = [2015]
ds = "USGS_NWIS_WU"
geo_level_fba = 'state'
# load FBA
usgs_water_fba_2015 = flowsa.getFlowByActivity(flowclass=fc, years=years_fba,
                                               datasource=ds, geographic_level=geo_level_fba).reset_index(drop=True)
# save output to csv
usgs_water_fba_2015.to_csv(fbaoutputpath + ds + "_" + "_".join(map(str, years_fba)) + ".csv", index=False)
