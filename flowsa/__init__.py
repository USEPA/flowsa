# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
"""
import yaml
import pandas as pd
from flowsa.common import outputpath, datapath, log

def getFlowByActivity(flowclass, years, datasource=None):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: `Class' of the flow. required. E.g. 'Water'
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource. If none returns data from all sources
    :return: a pandas DataFrame in FlowByActivity format
    """
    sources= datapath+'source_catalog.yaml'
    with open(sources, 'r') as f:
        config = yaml.safe_load(f)
    source_dict_by_class = config['flowbyactivity']
    try:
        sources = source_dict_by_class[flowclass]
    except KeyError:
        log.error("No FlowByActivity data found for flow class "+flowclass)
    if datasource is not None:
        sources = [datasource]
    class_flowbyactivity = pd.DataFrame()
    for s in sources:
        for y in years:
            try:
                flowbyactivity = pd.read_parquet(outputpath + s + "_" + str(y) + ".parquet")
                class_flowbyactivity = pd.concat([class_flowbyactivity,flowbyactivity], sort=False)
            except FileNotFoundError:
                log.error("No parquet file found for datasource "+ s + "and year " + str(y) + " in flowsa" )

    return class_flowbyactivity



