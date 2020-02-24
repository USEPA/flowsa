# data_pull.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import os
import io
import yaml
import pyarrow 
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

"""
Classes and methods for pulling data from a USGS web service.

Available functions:
generate_urls - generates the URL for the USGS water use data. 
get_header_general_and_data - takes usgs data file and parces the Header, Meta Data and Data out of the file. 
print_file - Joins all the data frames 1 from each file and concatinates the data frames into 1 data frame. This method  then prints the parquet file
call_urls - calls the url and gets the usgs data file. 
                This method calls the get_header_general_and_data method and the parse_header method in the data_reshape class. 
-
"""
def print_file(result):
    """Prints the data frame into a parquet file."""
    result.to_parquet('FlowByActivity.parquet', 'pyarrow')
    print_file(result)

def get_yaml():
    file = open('./flowsa/usgs_water_consume/datasource_config.yaml', 'r')
    document = yaml.full_load(file)
    return document