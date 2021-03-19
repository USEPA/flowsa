# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class. Method files found in
                       flowsa/data/flowbysectormethods
    :return: dataframe in flow by sector format
"""

import flowsa

# load a FBS from local directory
fbs_water = flowsa.getFlowBySector('Water_national_2015_m1')

# collapse the FBS - output has 'Sector' column instead of 'SectorProducedBy' and 'SectorConsumedBy' columns
fbs_water_collapsed = flowsa.collapse_FlowBySector('Water_national_2015_m2')
