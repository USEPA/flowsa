# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
"""
import yaml
import pandas as pd
from flowsa.common import outputpath, datapath, log

def getFlowByActivity(flowclass, years, datasource):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
    """
    fba = pd.DataFrame()
    for y in years:
        try:
            flowbyactivity = pd.read_parquet(outputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
            flowbyactivity = flowbyactivity[flowbyactivity['Class'].isin(flowclass)]
            fba = pd.concat([fba, flowbyactivity], sort=False)
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + datasource + "and year " + str(y) + " in flowsa")
    return fba




