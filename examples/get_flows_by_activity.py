# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
"""
Examples of use of flowsa. Read parquet files as dataframes.
"""
import flowsa

# "employment" based datasets
employ_bls_flowsbyactivity_2014 = flowsa.getFlowByActivity(flowclass='Employment', years=[2014],
                                                           datasource="BLS_QCEW_EMP")
employ_bls_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass='Employment', years=[2015],
                                                           datasource="BLS_QCEW_EMP")
employ_cpb_flowsbyactivity_2014 = flowsa.getFlowByActivity(flowclass='Employment', years=[2014],
                                                           datasource="Census_CBP_EMP")


# "land" based datasets
cropland_flowsbyactivity_2017 = flowsa.getFlowByActivity(flowclass='Land', years=['2017'])


# "money" based datasets
prodmarkvalue_coa_flowsbyactivity = flowsa.getFlowByActivity(flowclass='Money',years=[2012,2017],
                                                             datasource="USDA_CoA_ProdMarkValue")


# "other" based datasets
census_pop_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass='Other', years=[2015],
                                                           datasource="Census_Population")


# "water" based datasets
usgs_water_flowsbyactivity = flowsa.getFlowByActivity(flowclass='Water',years=[2010,2015],
                                                      datasource="USGS_Water_Use")
usgs_water_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass='Water', years=[2015],
                                                           datasource="USGS_Water_Use")
stat_canada_flowsbyactivity = flowsa.getFlowByActivity(flowclass='Water', years=[2015],
                                                       datasource="Stat_Canada")
eia_cbecs_flowsbyactivity_2012 = flowsa.getFlowByActivity(flowclass='Water', years=[2012],
                                                          datasource="EIA_CBECS")








