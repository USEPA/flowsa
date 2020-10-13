# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
from flowsa.common import fbaoutputpath, fbsoutputpath, datapath, log
from flowsa.flowbyfunctions import collapse_fbs_sectors


def getFlowByActivity(flowclass, years, datasource):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or
     ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
    """
    fbas = pd.DataFrame()
    # for assigning dtypes
    fields = {'ActivityProducedBy': 'str'}
    for y in years:
        # first try reading parquet from your local repo
        try:
            fba = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet")
            fba = fba[fba['Class'].isin(flowclass)]
            fba = fba.astype(fields)
            fbas = pd.concat([fbas, fba], sort=False)
        except OSError:
            # if parquet does not exist in local repo, read file from Data Commons
            try:
                fba = pd.read_parquet('https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowByActivity/' +
                                      datasource + "_" + str(y) + '.parquet')
                fba = fba[fba['Class'].isin(flowclass)]
                fba = fba.astype(fields)
                fbas = pd.concat([fbas, fba], sort=False)
            except FileNotFoundError:
                log.error("No parquet file found for datasource " + datasource + "and year " + str(
                    y) + " in flowsa or Data Commons")
    return fbas


def getFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = pd.DataFrame()
    # first try reading parquet from your local repo
    try:
        fbs = pd.read_parquet(fbsoutputpath + methodname + ".parquet")
    except OSError:
        # if parquet does not exist in local repo, read file from Data Commons
        try:
            fbs = pd.read_parquet('https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowBySector/' +
                                  methodname + ".parquet")
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + methodname + " in flowsa or Data Commons")

    return fbs


def getFlowBySector_collapsed(methodname):
    """
    Retrieves stored data in the FlowBySector format,
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """

    # load saved FBS parquet
    fbs = getFlowBySector(methodname)
    fbs_collapsed = collapse_fbs_sectors(fbs)

    return fbs_collapsed
