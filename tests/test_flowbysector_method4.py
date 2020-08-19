# test_flowbyactivityfunctions.py (tests)
# !/usr/bin/env python3
# coding=utf-8

""" Tests of flowbysectorfunctions """
import os
import unittest
import pandas as pd
from flowsa.flowbyfunctions import *
from flowsa.common import *

class TestFlowBySectorFunctions(unittest.TestCase):

    def setUp(self):
        path = fbsoutputpath
        flows = pd.read_parquet(fbsoutputpath + "testmethod4.parquet")
        self.flows = flows.fillna(value=fba_fill_na_dict)

    def test_aggregator(self):
        flows = self.flows
        flows = aggregator(flows, fbs_default_grouping_fields)
        totaltotaldomestic = flows['FlowAmount'].loc[(flows['Flowable'] == 'total') &
                                                     (flows['Context'] == 'total') &
                                                     (flows['SectorProducedBy'] == 'None') &
                                                     (flows['SectorConsumedBy'] == 'F010')].reset_index(drop=True)
        totaltotalpstodomestic = flows['FlowAmount'].loc[(flows['Flowable'] == 'total') &
                                                     (flows['Context'] == 'total') &
                                                     (flows['SectorProducedBy'] == '22') &
                                                     (flows['SectorConsumedBy'] == 'F010')].reset_index(drop=True)
        self.assertEqual(8000, totaltotaldomestic[0])
        self.assertEqual(4500, totaltotalpstodomestic[0])

    # def test_agg_by_geoscale(self):
    #     flows = self.flows
    #     flows = flows[flows['FlowName']=="RA-7"]
    #     flows = agg_by_geoscale(flows,"county","national")
    #     amount = flows['FlowAmount'][0]
    #     self.assertEqual(19000,amount)

