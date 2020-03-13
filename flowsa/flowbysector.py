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
from flowsa.flowbyactivity import filter_by_geographic_scale


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




#Need to create sums by flowtype and unit to check against later

#Add in Sector matches now to producing and consuming activities
#df = add_sectors_to_flowbyactivity(df)
#Very important to note that values have not been allocated but are repeated at this point


# Removing records that we don't need

def main(method_name):
    """
    Creates a flowbysector dataset
    :param method_name: Name of method corresponding to flowbysector method yaml name
    :return: flowbysector
    """

    method = load_method(method_name)
    fbas = [method['flowbyactivity_sources']]
    for fba in fbas:
        flows = flowsa.getFlowByActivity(flowclass=fba['class'],
                                                    years=[fba['year']],
                                                    datasource = fba['name'])
        flows = filter_by_geographic_scale(flows,geoscale=method['target_geographic_scale'])
        flows = add_sectors_to_flowbyactivity(flows,sectorsourcename=method['target_sector_source'])

    return flows

















