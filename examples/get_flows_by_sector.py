# get_flows_by_sector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Flow-By-Sector (FBS) datasets are environmental, economic, and other data that
are attributed to economic sectors, generally by North American Industrial
Classification (NAICS) Codes. These datasets capture env/econ flows from sectors
that produce to the sectors that consume.
For example, Water_national_2015_m1 (https://github.com/USEPA/flowsa/blob/
master/flowsa/methods/flowbysectormethods/Water_national_2015_m1.yaml)
captures the flow of withdrawn water through the economy. This dataset
tracks water withdrawn by Public Supply (SectorProducedBy) that flows to
Domestic use (SectorConsumedBy).

Not all FBS contain data in both the
SectorProducedBy and SectorConsumedBy columns. For example
Employment_national_2018 (https://github.com/USEPA/flowsa/blob/master/
flowsa/methods/flowbysectormethods/Employment_national_2018.yaml) only contains
employment data in the SectorProducedBy column, as there are not flows of
employment between sectors.

Tables are standardized into a table format defined in
https://github.com/USEPA/flowsa/blob/master/format%20specs/FlowBySector.md.

`getFlowBySector()` has required and optional parameters
    :param methodname: str, name of an available method
    :param fbsconfigpath: str, path to the FBS method file if loading a file
        from outside the flowsa repository, optional
    :param download_FBAs_if_missing: bool, if True will attempt to load FBAs
        used in generating the FBS from remote server prior to generating if
        file not found locally, optional, default is False
    :param download_FBS_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally,
        optional, default is False
    :return: dataframe in flow by sector format

"""
from flowsa import getFlowBySector, collapse_FlowBySector, \
    seeAvailableFlowByModels

# see available FBS models
seeAvailableFlowByModels('FBS')

# load FBS from local directory, if does not exist, method will run, option
# to download the FBAs from Data Commons
# (https://dmap-data-commons-ord.s3.amazonaws.com/index.html?prefix=flowsa/)
# to run the method
fbs_water = getFlowBySector('Water_national_2015_m1',
                            download_FBAs_if_missing=True)

# collapse the FBS - output has 'Sector' column instead of
# 'SectorProducedBy' and 'SectorConsumedBy' columns. The collapsed
# `Water_national_2015_m1` FBS will have 2 fewer rows, as the df is aggregated
# after dropping "SectorProducedBy" information
fbs_water_collapsed = collapse_FlowBySector('Water_national_2015_m1')
