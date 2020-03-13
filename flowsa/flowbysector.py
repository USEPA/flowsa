# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class
"""
import pandas as pd
import flowsa
import yaml
from flowsa.common import log, flowbyactivitymethodpath
from flowsa.mapping import add_sectors_to_flowbyactivity


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name:
    :return:
    """
    sfile = flowbyactivitymethodpath+method_name+'.yaml'
    try:
        with open(sfile, 'r') as f:
            method = yaml.safe_load(f)
    except IOError:
        log.error("File not found.")
    return method


def get_source_flowbyactivity(flowclass,source_list):
    flowbyactivities = []
    for s in source_list:
        flowbyactivity = flowsa.getFlowByActivity(flowclass, s)
        flowbyactivities.append(flowbyactivity)
    try:
        flowbyactivity_df = pd.concat(flowbyactivities, ignore_index=True)
    except:
        log.error("Failed to merge flowbyactivity files.")
    return flowbyactivity_df


#df = get_source_flowbyactivity(flowclass,source_list)

#Need to create sums by flowtype and unit to check against later

#Add in Sector matches now to producing and consuming activities
#df = add_sectors_to_flowbyactivity(df)
#Very important to note that values have not been allocated but are repeated at this point


# Removing records that we don't need


# Unit conversion to m3

















