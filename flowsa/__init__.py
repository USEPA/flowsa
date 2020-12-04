# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
from flowsa.processed_data_mgmt import load_preprocessed_output
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
    for y in years:
        fba_file = "FlowByActivity/"+ datasource + "_" + str(y) + ".parquet"
        fba = load_preprocessed_output(fba_file)
        fba = fba[fba['Class'].isin(flowclass)]
        fbas = pd.concat([fbas, fba], sort=False)
    return fbas


def getFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs_file = "FlowBySector/" + methodname + ".parquet"
    fbs = load_preprocessed_output(fbs_file)
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
