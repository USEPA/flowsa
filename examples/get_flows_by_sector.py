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

# load a FBS from local folder
fbs_water = flowsa.getFlowBySector('Water_national_2015_m1')

# load a FBS from remote server
fbs_water_remote = flowsa.getFlowBySector('Water_national_2015_m1', file_location='remote')

# collapse the FBS - output has 'Sector' column instead of 'SectorProducedBy' and 'SectorConsumedBy' columns
fbs_water_collapsed = flowsa.getFlowBySector_collapsed('Water_national_2015_m1')


