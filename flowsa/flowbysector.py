# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces one or more FlowBySector files based on a method file for the given class
"""
import flowsa
from flowsa.common import log
from flowsa.mapping import add_sectors_to_flowbyactivity


def get_source_flowbyactivity(flowclass,source_list):
    flowbyactivities = []
    for s in source_list:
        flowbyactivity = flowsa.getFlowByActivity(flowclass, s)
        flowbyactivities.append(flowbyactivity)
    try:
        flowbyactivity_df = pd.concat(flowbyactivities, sort=False)
    except:
        log.error("Failed to merge flowbyactivity files.")
    return flowbyactivity_df


# Removing records that we don't need


# Unit conversion to m3


##Map to FlowBySector
#df = get_source_flowbyactivity(flowclass,source_list)
#df = add_sectors_to_flowbyactivity(df)














