# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls QCEW data from BLS
"""

import io
import pandas as pd
import requests
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request
from flowsa.common import log, flow_by_activity_fields

def get_bls_qcew(year, state):
    """
    This function pulls QCEW data by year by state from BLS
    """
    area_fips = getFIPS(state, county = None)
    url = base_url + str(year) + "/a/area/" + area_fips + ".csv"
    r = make_http_request(url).content
    QCEW = pd.read_csv(io.StringIO(r.decode('utf-8')))[["area_fips", "own_code",
                                                        "industry_code", "year",
                                                        "annual_avg_estabs",
                                                        "annual_avg_emplvl"]]
    # create a flag if owner code = 5 (private ownership) but has 0 employment
    QCEW["flag"] = "no manual check needed"
    QCEW.loc[(QCEW.own_code == 5) & (QCEW.annual_avg_emplvl == 0), "flag"] = "private ownership but employment is 0"
    # keep owner_code = 1, 2, 3, 5
    QCEW = QCEW[QCEW.own_code.isin([1, 2, 3, 5])]
    # aggregate annual_avg_estabs and annual_avg_emplvl by area_fips, industry_code, year, flag
    QCEW = QCEW.groupby(["area_fips",
                         "industry_code",
                         "year",
                         "flag"])[["annual_avg_estabs", "annual_avg_emplvl"]].sum().reset_index()
    # adjust area_fips in QCEW
    if len(str(QCEW.area_fips[1])) == 4:
        QCEW.area_fips = str(0) + str(QCEW.area_fips[1])
    # assign state name
    QCEW["area_title"] = state
    # adjust state name
    QCEW[QCEW.area_fips == "US000", "area_title"] = "United States"
    # re-order columns
    QCEW = QCEW[["area_fips", "area_title", "industry_code", "year", "annual_avg_estabs", "annual_avg_emplvl", "flag"]]
    return QCEW
