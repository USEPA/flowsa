# get_flows_by_sector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Flow-By-Sector (FBS) datasets are environmental, economic, and other data that
are attributed to economic sectors, generally North American Industrial
Classification (NAICS) Codes. These datasets capture flows of env/econ data
from sectors that produce the data to the sectors that consume the data.
For example, Water_national_2015_m1 (https://github.com/USEPA/flowsa/blob/
master/flowsa/methods/flowbysectormethods/Water_national_2015_m1.yaml)
captures the flow of withdrawn water through the economy. This dataset
tracks water withdrawn by Public Supply (SectorProducedBy) that flows to
Domestic use (SectorConsumedBy). Not all FBS contain data in both the
SectorProducedBy and SectorConsumedBy columns. For example
Employment_national_2018 (https://github.com/USEPA/flowsa/blob/master/
flowsa/methods/flowbysectormethods/Employment_national_2018.yaml) only contains
employment data in the SectorProducedBy column, as there are not flows of
employment between sectors.

Tables are standardized into a table defined in
https://github.com/USEPA/flowsa/blob/master/format%20specs/FlowBySector.md.

Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class.
    Method files found in flowsa/data/flowbysectormethods
    :return: dataframe in flow by sector format

"""

import flowsa

# see available FBS models
flowsa.seeAvailableFlowByModels('FBS')

# load FBS from local directory, if does not exist, method will run
fbs_water = flowsa.getFlowBySector('Water_national_2015_m1',
                                   download_FBAs_if_missing=True)

# collapse the FBS - output has 'Sector' column instead of
# 'SectorProducedBy' and 'SectorConsumedBy' columns
fbs_water_collapsed = flowsa.collapse_FlowBySector('Water_national_2015_m1')
