# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
"""
Examples of use of flowsa. Read parquet files as dataframes.
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
"""
import flowsa

# "employment" based datasets
employ_bls_flowsbyactivity_2012 = flowsa.getFlowByActivity(flowclass=['Employment'], years=[2012],
                                                           datasource="BLS_QCEW")
employ_bls_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass=['Employment', 'Money'], years=[2015],
                                                           datasource="BLS_QCEW")
employ_cpb_flowsbyactivity_2012 = flowsa.getFlowByActivity(flowclass=['Employment', 'Other'], years=[2012],
                                                           datasource="Census_CBP")


# "land" based datasets
cropland_flowsbyactivity_2017 = flowsa.getFlowByActivity(flowclass=['Land'], years=[2017],
                                                         datasource="USDA_CoA_Cropland")

# "money" based datasets
fisheries_noaa_flowsbyactivity = flowsa.getFlowByActivity(flowclass=['Money'], years=["2012-2018"],
                                                          datasource="NOAA_FisheryLandings")


# "other" based datasets
census_pop_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass=['Other'], years=[2015],
                                                           datasource="Census_PEP_Population")
livestock_flowsbyactivity_2017 = flowsa.getFlowByActivity(flowclass=['Other'], years=[2017],
                                                          datasource="USDA_CoA_Livestock")


# "water" based datasets
eia_cbecs_flowsbyactivity_2012 = flowsa.getFlowByActivity(flowclass=['Water'], years=[2012],
                                                          datasource="EIA_CBECS_Water")
stat_canada_flowsbyactivity = flowsa.getFlowByActivity(flowclass=['Water'], years=["2005-2015"],
                                                       datasource="StatCan_IWS_MI")
usda_iwms_flowsbyactivity_2013 = flowsa.getFlowByActivity(flowclass=['Water'], years=[2013],
                                                          datasource="USDA_IWMS")
usgs_water_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass=['Water'], years=[2015],
                                                           datasource="USGS_NWIS_WU")
usgs_water_flowsbyactivity = flowsa.getFlowByActivity(flowclass=['Water'], years=[2010, 2015],
                                                      datasource="USGS_NWIS_WU")








