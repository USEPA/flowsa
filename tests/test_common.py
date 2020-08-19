# test_flowbyactivityfunctions.py (tests)
# !/usr/bin/env python3
# coding=utf-8

""" save flowbyactivity test data as parquet files """

import pandas as pd
import os
import logging as log
from flowsa.common import flow_by_activity_fields, fbaoutputpath, datapath
from flowsa.flowbyfunctions import add_missing_flow_by_fields


def store_test_flowbyactivity(csvname, year=None):
    """Prints the data frame into a parquet file."""
    if year is not None:
        f = fbaoutputpath + csvname + "_" + str(year) + '.parquet'
    else:
        f = fbaoutputpath + csvname + '.parquet'
    try:
        result = pd.read_csv(datapath + csvname + '.csv', dtype='str')
        result = add_missing_flow_by_fields(result, flow_by_activity_fields)
        result.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save ' + csvname + "_" + str(year) + ' file.')


def gettestFlowByActivity(flowclass, years, datasource):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or
     ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
    """
    fbas = pd.DataFrame()
    for y in years:
        try:
            fba = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet")
            fba = fba[fba['Class'].isin(flowclass)]
            fbas = pd.concat([fbas, fba], sort=False)
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + datasource + "and year " + str(
                y) + " in flowsa")
    return fbas


def gettestFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = pd.DataFrame()
    try:
        fbs = pd.read_parquet(fbsoutputpath + methodname + ".parquet")
    except FileNotFoundError:
        log.error("No parquet file found for datasource " + methodname + " in flowsa")
    return fbs
