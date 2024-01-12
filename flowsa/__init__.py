# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for flowsa
For standard dataframe formats, see
https://github.com/USEPA/flowsa/tree/master/format%20specs

Files are loaded from a user's local directory
https://github.com/USEPA/flowsa/wiki/Data-Storage#local-storage

or can be downloaded from a remote repository
https://dmap-data-commons-ord.s3.amazonaws.com/index.html?prefix=flowsa/

The most recent version (based on timestamp) of Flow-By-Activity and
Flow-By-Sector files are loaded when running these functions
"""

from flowsa.common import seeAvailableFlowByModels
from flowsa.flowbyactivity import getFlowByActivity
from flowsa.flowbysector import getFlowBySector, collapse_FlowBySector
from flowsa.datavisualization import (FBSscatterplot, stackedBarChart,
                                      plot_state_coefficients)
# from flowsa.bibliography import writeFlowBySectorBibliography

