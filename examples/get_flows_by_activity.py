# get_flows_by_activity.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Flow-By-Activity (FBA) datasets are environmental and economic data that are
generally pulled from publicly available data sources and formatted into
standardized tables (defined in
https://github.com/USEPA/flowsa/blob/master/format%20specs/FlowByActivity.md).
These data are generally unchanged from the source data, with the exception
of formatting.

See source_catalog.yaml for available FlowByActivity datasets and
available parameters for getFlowByActivity().
Examples of use of flowsa. Read parquet files as dataframes.
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str, a 'Class' of the flow. Optional. E.g. 'Water'
    :param geographic_level: str, a geographic level of the data.
    Optional. E.g. 'national', 'state', 'county'.
    :return: a pandas DataFrame in FlowByActivity format

"""

from flowsa.common import seeAvailableFlowByModels
import flowsa.flowbyactivity
from flowsa.settings import fbaoutputpath

# see all datasources and years available in flowsa
seeAvailableFlowByModels('FBA')

# Load all information for EIA MECS Land
fba_mecs = flowsa.flowbyactivity.getFlowByActivity(datasource="EIA_MECS_Land", year=2014)

# only load state level water data and save as csv
fba_usgs = flowsa.flowbyactivity.getFlowByActivity(datasource="USGS_NWIS_WU",
                                                   year=2015,
                                                   flowclass='Water',
                                                   geographic_level='state'
                                                   )

# save output to csv, maintain leading 0s in location col
fba_usgs.Location = fba_usgs.Location.apply('="{}"'.format)
fba_usgs.to_csv(fbaoutputpath / "USGS_NWIS_WU_2015.csv", index=False)
