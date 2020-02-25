# usgs_water_consume.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces one or more FlowBySector files for the Water class
"""
import flowsa
from flowsa.mapping import map_activities_to_sector

wat_flowbyactivity = flowsa.getFlowByActivity('Water')

# Removing records that we don't need



# Unit conversion to m3


##Map to FlowBySector
#map_activities_to_sector(wat_flowbyactivity)














