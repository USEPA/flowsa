# get_flows_by_sector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class.
    Method files found in flowsa/data/flowbysectormethods
    :return: dataframe in flow by sector format
"""

import flowsa
from flowsa.datachecks import compare_FBS_results

# load FBS from local directory, if does not exist, method will run
fbs_water = flowsa.getFlowBySector('Water_state_2015_m1')

# collapse the FBS - output has 'Sector' column instead of
# 'SectorProducedBy' and 'SectorConsumedBy' columns
fbs_water_collapsed = flowsa.collapse_FlowBySector('Water_national_2015_m1')

# compare two FBS
fbs1 = 'Land_national_2012_v0.1_864d573'
fbs2 = 'Land_national_2012'
fbs_compare = compare_FBS_results(fbs1, fbs2)
