# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class
"""
import pandas as pd
import flowsa
from flowsa.common import log
from flowsa.mapping import add_sectors_to_flowbyactivity


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

flowclass = 'Water'
source_list = ['USGS_Water_Use']
df = get_source_flowbyactivity(flowclass,source_list)

#Need to create sums by flowtype and unit to check against later

#Add in Sector matches now to producing and consuming activities
df = add_sectors_to_flowbyactivity(df)
#Very important to note that values have not been allocated but are repeated at this point


# Removing records that we don't need


# Unit conversion to m3

















