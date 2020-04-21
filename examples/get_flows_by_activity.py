# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
"""
Examples of use of flowsa
"""
import flowsa

waterflowsbyactivity = flowsa.getFlowByActivity(flowclass='Water',years=[2010,2015],
                                                datasource="USGS_Water_Use")
waterflowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass='Water', years=[2015],
                                                datasource="USGS_Water_Use")
employ_bls_flowsbyactivity_2014 = flowsa.getFlowByActivity(flowclass='Employment', years=[2014],
                                                     datasource="BLS_QCEW_EMP")
employ_cpb_flowsbyactivity_2014 = flowsa.getFlowByActivity(flowclass='Employment', years=[2014],
                                                     datasource="Census_CBP_EMP")
employ_bls_flowsbyactivity_2015 = flowsa.getFlowByActivity(flowclass='Employment', years=[2015],
                                                     datasource="BLS_QCEW_EMP")
land_2017 = flowsa.getFlowByActivity(flowclass='Land', years=['2017'])
prodmarkvalue_flowsbyactivity = flowsa.getFlowByActivity(flowclass='Money',years=[2012,2017],
                                                         datasource="USDA_CoA_ProdMarkValue")






