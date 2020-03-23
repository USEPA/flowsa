# test_flowbyactivity.py (tests)
# !/usr/bin/env python3
# coding=utf-8

""" Tests of flowbyactivity functions """
import os
import unittest
import pandas as pd
from flowsa.flowbyactivity import *

class TestFlowByActivity(unittest.TestCase):

    def setUp(self):
        path = os.path.dirname(__file__) + './data/'
        self.flows = pd.read_csv(path+"StarWars.csv")
        print(self.flows.columns)

    def test_aggregator(self):
        flows = self.flows
        flows = flows[flows['FlowName']=="X-wing"]
        groupbycols = ['FlowName','Compartment']
        flows = aggregator(flows,groupbycols)
        amount = flows['FlowAmount'][0]
        print("Amount of X-wings after aggregation is " + str(amount))
        self.assertEqual(320,amount)

    def test_agg_by_geoscale(self):
        flows = self.flows
        flows = flows[flows['FlowName']=="RA-7"]
        flows = agg_by_geoscale(flows,"county","national")
        amount = flows['FlowAmount'][0]
        self.assertEqual(19000,amount)

