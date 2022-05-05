# stateio.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Supporting functions for accessing files from stateior via data commons.
https://github.com/USEPA/stateior
"""

import os
import pandas as pd

from flowsa.metadata import set_fb_meta
from flowsa.location import us_state_abbrev, apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system

from esupy.processed_data_mgmt import download_from_remote, Paths,\
    load_preprocessed_output


def parse_statior(*, source, year, config, **_):
    """parse_response_fxn for stateio make and use tables
    """
    # Prepare meta for downloading stateior datasets
    name = config.get('datatype')
    fname = f"{name}_{year}"
    meta = set_fb_meta(fname, "")
    meta.tool = 'stateio'
    meta.ext = 'rds'
    stateio_paths = Paths()
    stateio_paths.local_path = os.path.realpath(stateio_paths.local_path + "/stateio")

    # Download and load the latest version from remote
    download_from_remote(meta, stateio_paths)
    states = load_preprocessed_output(meta, stateio_paths)

    use_dict = {}

    # uses rpy2
    # this .rds is stored as a list of named dataframes by state
    for state in us_state_abbrev.keys():
         df = states.rx2(state)
         df2 = df.melt(ignore_index=False, value_name = 'FlowAmount',
                       var_name = 'ActivityConsumedBy',
                       )
         df2['ActivityProducedBy'] = df2.index
         if source == 'stateio_Make_Summary':
             # Adjust the index by removing the state: STATE.SECTOR
             df2['ActivityProducedBy'] = df2[
                 'ActivityProducedBy'].str.split(".", expand=True)[1]
         df2.reset_index(drop=True, inplace=True)
         df2['State'] = state
         use_dict[state] = df2

    fba = pd.concat(use_dict, ignore_index=True)
    fba.dropna(subset=['FlowAmount'], inplace=True)

    # Gross Output
    if 'GO' in source and 'ActivityConsumedBy' in fba.columns:
        fba = fba.drop(columns=['ActivityConsumedBy'])

    # Assign location
    fba['County'] = ''
    fba = apply_county_FIPS(fba)
    fba = assign_fips_location_system(fba, '2015')
    fba.drop(columns=['County'], inplace=True)

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


def subset_stateio_table(df, attr, **_):
    """Subset the stateio make or use table using BEA function"""
    from flowsa.data_source_scripts.BEA import subset_BEA_table
    df = subset_BEA_table(df, attr)
    return df


if __name__ == "__main__":
    import flowsa
    source = 'stateio_Industry_GO'
    flowsa.flowbyactivity.main(year=2017, source=source)
    fba = flowsa.getFlowByActivity(source, 2017)