# test_flowbyactivityfunctions.py (tests)
# !/usr/bin/env python3
# coding=utf-8

""" Tests of flowbyactivity functions """
import os
import unittest
import pandas as pd
from flowsa.common import outputpath

class TestFlowByActivityFunctions(unittest.TestCase):

    def setUp(self):
        self.fbas = []
        for f in os.listdir(outputpath):
            print(f)
            fba = pd.read_parquet(f)
            self.fbas.append(fba)

    def test_fields_and_data_types(self):
        for fba in self.fbas:
            for s in fba:
                print(s.dtype)


