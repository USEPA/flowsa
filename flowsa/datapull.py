# datapull.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
"""
import yaml
import requests
from flowsa.common import outputpath, sourceconfigpath, log

def store_flowbyactivity(result, source):
    """Prints the data frame into a parquet file."""
    try:
        result.to_parquet(outputpath + source +'.parquet', 'pyarrow')
    except:
        log.error('Failed to save '+source+' file.')

def make_http_request(url):
    try:
        r = requests.get(url)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    return r

def load_sourceconfig(source):
    sfile = sourceconfigpath+source+'.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config

