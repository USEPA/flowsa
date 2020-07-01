# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Equations for calculating data quality scores, based on Edelen's "The creation, management, and use of data quality
information for LCA" (2017).

"""
import flowsa
import pandas as pd
import yaml
from flowsa.common import log, flowbyactivitymethodpath, flow_by_sector_fields, \
    generalize_activity_field_names, outputpath, datapath
from flowsa.mapping import add_sectors_to_flowbyactivity, get_fba_allocation_subset
from flowsa.flowbyfunctions import fba_activity_fields, fbs_default_grouping_fields, agg_by_geoscale, \
    fba_fill_na_dict, fbs_fill_na_dict, convert_unit, fba_default_grouping_fields, \
    add_missing_flow_by_fields, fbs_activity_fields, allocate_by_sector, allocation_helper, sector_aggregation
from flowsa.USGS_NWIS_WU import standardize_usgs_nwis_names

flow_data_quality_fields = ['Reliability_Score','TemporalCorrelation','GeographicalCorrelation',
                            'TechnologicalCorrelation','DataCollection']
