# stateio.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting functions for accessing files from stateior via data commons.
https://github.com/USEPA/stateior
"""

import os
import pandas as pd

from esupy.processed_data_mgmt import download_from_remote, Paths,\
    load_preprocessed_output

import flowsa.flowbyactivity
from flowsa.metadata import set_fb_meta
from flowsa.location import us_state_abbrev, apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def parse_statior(*, source, year, config, **_):
    """parse_response_fxn for stateio make and use tables"""
    # Prepare meta for downloading stateior datasets
    name = config.get('datatype')
    fname = f"{name}_{year}_{config.get('version')}"
    meta = set_fb_meta(fname, "")
    meta.tool = 'stateio'
    meta.ext = 'rds'
    stateio_paths = Paths()
    stateio_paths.local_path = stateio_paths.local_path / "stateio"
    # Download and load the latest version from remote
    download_from_remote(meta, stateio_paths)
    states = load_preprocessed_output(meta, stateio_paths)

    data_dict = {}

    # uses rpy2, known to work with 3.5.12 and python 3.12
    # this .rds is stored as a list of named dataframes by state
    for state in us_state_abbrev.keys():
         matrix = states.rx2(state)
         df = pd.DataFrame(matrix).transpose()
         df.columns = list(matrix.colnames)
         df['col'] = list(matrix.rownames)
         df = df.set_index('col')
         df2 = df.melt(ignore_index=False, value_name = 'FlowAmount',
                       var_name = 'ActivityConsumedBy')
         df2['ActivityProducedBy'] = df2.index
         if source == 'stateio_Make_Summary':
             # Adjust the index by removing the state: STATE.SECTOR
             df2['ActivityProducedBy'] = df2[
                 'ActivityProducedBy'].str.split(".", expand=True)[1]
         df2 = df2.reset_index(drop=True)
         df2['State'] = state
         data_dict[state] = df2

    fba = pd.concat(data_dict, ignore_index=True)
    fba = fba.dropna(subset=['FlowAmount'])

    # Gross Output
    if 'GO' in source and 'ActivityConsumedBy' in fba.columns:
        fba = fba.drop(columns=['ActivityConsumedBy'])

    # Assign location
    fba = apply_county_FIPS(fba)
    fba = assign_fips_location_system(fba, '2015')
    fba = fba.drop(columns=['County'])

    # Hardcoded data
    fba['Year'] = year
    fba['SourceName'] = source
    fba['Class'] = 'Money'
    fba['Unit'] = "USD"
    fba['FlowName'] = f"USD{year}"
    fba["FlowType"] = "TECHNOSPHERE_FLOW"
    fba['DataReliability'] = 5  # tmp
    fba['DataCollection'] = 5  # tmp
    return fba


if __name__ == "__main__":
    import flowsa
    # source = 'stateio_Industry_GO'
    # source = 'stateio_Make_Summary'
    source = 'stateio_Use_Summary'
    for y in range(2012, 2024):
        flowsa.generateflowbyactivity.main(year=y, source=source)
    fba = flowsa.flowbyactivity.getFlowByActivity(source, 2022)
