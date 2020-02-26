# water_use.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces one or more FlowBySector files for the Water Use class
"""
import flowsa
from flowsa.mapping import map_activities_to_sector

wat_flowbyactivity = flowsa.getFlowByActivity(flowclass='Water')

# Removing records that we don't need



# Unit conversion to m3


##Map to FlowBySector
wat_flowbysector = map_activities_to_sector(wat_flowbyactivity)














