"""
Test functions work
"""

import flowsa


def test_get_flows_by_activity():
    flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014)


def test_get_flows_by_sector():
    # set function to download any FBAs that are missing
    flowsa.getFlowBySector('Water_national_2015_m1', download_FBAs_if_missing=True)


def test_write_bibliography():
    flowsa.writeFlowBySectorBibliography('Water_national_2015_m1')
