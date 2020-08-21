# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
from flowsa.common import fbaoutputpath, fbsoutputpath, datapath, log
from flowsa.flowbysector import collapse_fbs_sector_columns


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
    fields = {'ActivityProducedBy':'str'}
    for y in years:
        try:
            fba = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet")
            fba = fba[fba['Class'].isin(flowclass)]
            fba = fba.astype(fields)
            fbas = pd.concat([fbas, fba], sort=False)
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + datasource + "and year " + str(
                y) + " in flowsa")
    return fbas


def getFlowBySector(methodname):
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


def getFlowBySector_collapsed(methodname):
    """
    Retrieves stored data in the FlowBySector format, collapsing the Sector Produced/Consumed By columns into a single
    column named "Sector"
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """

    fbs = getFlowBySector(methodname)
    fbs_collapsed = collapse_fbs_sector_columns(fbs, methodname)

    return fbs_collapsed
