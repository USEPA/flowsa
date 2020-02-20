# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov
import yaml
import pandas as pd
from flowsa.common import outputpath, datapath, log

def getFlowByActivity(flowclass, datasource=None, year=None):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: `Class' of the flow. required. E.g. 'Water'
    :param datasource:
    :param year:
    :return: a pandas DataFrame in FlowByActivity format
    """
    sources= datapath+'source_catalog.yaml'
    with open(sources, 'r') as f:
        config = yaml.safe_load(f)
    source_dict_by_class = config['flowbyactivity']
    try:
        class_sources = source_dict_by_class[flowclass]
    except KeyError:
        log.error("No FlowByActivity data found for flow class "+flowclass)

    class_flowbyactivity = pd.DataFrame()
    for s in class_sources:
        try:
            flowbyactivity = pd.read_parquet(outputpath + s + ".parquet")
            class_flowbyactivity = pd.concat([class_flowbyactivity,flowbyactivity], sort=False)
        except FileNotFoundError:
            log.error("No parquet file found for datasource "+ s + " in flowsa" )

    return class_flowbyactivity



