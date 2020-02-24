# data_reshape.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import sys, os, inspect, io
import pandas as pd
currentdir =  os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from common import flow_by_activity_fields

"""
Classes and methods for reshaping pulled data.

Available functions:
process_data - takes the lists that are dirived from the header and the data.
                        It then parses the data and puts it into a data table the data table.
                        The data table is formated into a panda data frame so it can be formated into a parquet file later.
split_name - splits the name into source and flow name
compartment - sets the compartment string
activity - sets the activity string
flow_type - sets the flow type
parse_header - takes the header and parses it into several lists for the Flow by activity format.
-
"""


def flw_name(name):
    """Sets the flow name based on it's name"""
    if"fresh" in name.lower():
        flow_name = "fresh"
    elif "saline" in name.lower():
        flow_name = "saline"
    else:
        flow_name = None
    return flow_name

def compartment(name):
    """Sets the compartment based on it's name"""
    if"surface" in name.lower():
        compartment = "surface"
    elif "ground" in name.lower():
        compartment = "ground"
    else:
        compartment = "blank"
    return compartment

def flow_type(name, technosphere_flow_array, waste_flow_array):
    """Takes the header and assigns one of three flow types.
    Everything starts as elementry but if there are keywords for technosphere and waste flow.
    The keywords are set in in the datasource_config.yaml """
    flow_type = "ELEMENTARY_FLOW"
    for t in technosphere_flow_array:
        if t in name:
            flow_type = "TECHNOSPHERE_FLOW"
    for w in waste_flow_array:
        if w in name:
             flow_type = "WASTE_FLOW"
    return flow_type

