# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
from esupy.processed_data_mgmt import load_preprocessed_output
from flowsa.common import paths
from flowsa.flowbyfunctions import collapse_fbs_sectors
import flowsa.flowbyactivity

def getFlowByActivity(flowclass, year, datasource):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or
     ['Land', 'Other']
    :param year: int, a year, e.g. 2012
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
    """
    #Set fba metadata
    fba_meta = flowsa.flowbyactivity.set_fba_meta(datasource,year)

    # Try to load a local version of fba; generate and load if missing
    fba = load_preprocessed_output(fba_meta,paths)
    if fba is None:
        # Generate the fba
        flowsa.flowbyactivity.main(year=year,source=datasource)
        # Now load the fba
        fba = load_preprocessed_output(fba_meta,paths)
    # Filter fba by flow class
    fba = fba[fba['Class'].isin(flowclass)]
    return fba


def getFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs_file = "FlowBySector/" + methodname + ".parquet"
    fbs = load_preprocessed_output(fbs_file, paths)
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
