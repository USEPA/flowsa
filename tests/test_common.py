# test_flowbyactivityfunctions.py (tests)
# !/usr/bin/env python3
# coding=utf-8

""" save flowbyactivity test data as parquet files """

import sys
import os
import yaml
import requests
import pandas as pd
import numpy as np
import logging as log
import appdirs

log.basicConfig(level=log.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
try:
    testmodulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    testmodulepath = 'flowsa/'


testdatapath = testmodulepath + 'tests/data/'
testsourceconfigpath = testdatapath + 'tests/sourceconfig/'
testoutputpath = testmodulepath + 'tests/output/'
testfbaoutputpath = testoutputpath + 'FlowByActivity/'
testfbsoutputpath = testoutputpath + 'FlowBySector/'
testflowbyactivitymethodpath = testdatapath + 'flowbysectormethods/'

def store_test_flowbyactivity(csvname, year=None):
    """Prints the data frame into a parquet file."""
    if year is not None:
        f = testfbaoutputpath + csvname + "_" + str(year) + '.parquet'
    else:
        f = testfbaoutputpath + csvname+ '.parquet'
    try:
        result = pd.read_csv(testdatapath + csvname + '.csv', dtype='str')
        result.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save '+ csvname + "_" + str(year) +' file.')


# store csv test data as parquet files
store_test_flowbyactivity('test_dataset_1_aquaculture', '2015')
store_test_flowbyactivity('test_dataset_2_irrigation_crop', '2015')
# store_test_flowbyactivity('test_dataset_3', '2015')