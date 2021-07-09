# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import logging as log
from esupy.processed_data_mgmt import load_preprocessed_output
from flowsa.common import paths, set_fb_meta, biboutputpath, fbaoutputpath, fbsoutputpath,\
    default_download_if_missing
from flowsa.flowbyfunctions import collapse_fbs_sectors, filter_by_geoscale
from flowsa.datachecks import check_for_nonetypes_in_sector_col, check_for_negative_flowamounts
import flowsa.flowbyactivity
import flowsa.flowbysector
from flowsa.bibliography import generate_fbs_bibliography


def getFlowByActivity(datasource, year, flowclass=None, geographic_level=None,
                      download_if_missing=default_download_if_missing):
    """
    Retrieves stored data in the FlowByActivity format
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str, a 'Class' of the flow. Optional. E.g. 'Water'
    :param geographic_level: str, a geographic level of the data.
                             Optional. E.g. 'national', 'state', 'county'.
    :param download_if_missing: bool, if True will attempt to load from remote server
        prior to generating if file not found locally
    :return: a pandas DataFrame in FlowByActivity format
    """
    from esupy.processed_data_mgmt import download_from_remote
    # Set fba metadata
    name = flowsa.flowbyactivity.set_fba_name(datasource, year)
    fba_meta = set_fb_meta(name, "FlowByActivity")

    # Try to load a local version of fba; generate and load if missing
    fba = load_preprocessed_output(fba_meta, paths)
    # Remote download
    if fba is None and download_if_missing:
        log.info(datasource + ' ' + str(year) + ' not found in ' + fbaoutputpath +
                 ', downloading from remote source')
        download_from_remote(fba_meta,paths)
        fba = load_preprocessed_output(fba_meta,paths)

    if fba is None:
        log.info(datasource + ' ' + str(year) + ' not found in ' +
                 fbaoutputpath + ', running functions to generate FBA')
        # Generate the fba
        flowsa.flowbyactivity.main(year=year, source=datasource)
        # Now load the fba
        fba = load_preprocessed_output(fba_meta, paths)
        if fba is None:
            log.error('getFlowByActivity failed, FBA not found')
        else:
            log.info('Loaded ' + datasource + ' ' + str(year) + ' from ' + fbaoutputpath)
    else:
        log.info('Loaded ' + datasource + ' ' + str(year) + ' from ' + fbaoutputpath)

    # Address optional parameters
    if flowclass is not None:
        fba = fba[fba['Class'] == flowclass]
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)
    return fba


def getFlowBySector(methodname):
    """
    Loads stored FlowBySector output or generates it if it doesn't exist, then loads
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs_meta = set_fb_meta(methodname, "FlowBySector")
    fbs = load_preprocessed_output(fbs_meta, paths)
    if fbs is None:
        log.info(methodname + ' not found in ' + fbsoutputpath +
                 ', running functions to generate FBS')
        # Generate the fba
        flowsa.flowbysector.main(method=methodname)
        # Now load the fba
        fbs = load_preprocessed_output(fbs_meta,paths)
        if fbs is None:
            log.error('getFlowBySector failed, FBS not found')
        else:
            log.info('Loaded ' + methodname + ' from ' + fbsoutputpath)
    else:
        log.info('Loaded ' + methodname + ' from ' + fbsoutputpath)
    return fbs


def collapse_FlowBySector(methodname):
    """
    Returns fbs with one sector column in place of two
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = flowsa.getFlowBySector(methodname)
    fbs_collapsed = collapse_fbs_sectors(fbs)

    # check data for NoneType in sector column
    fbs_collapsed = check_for_nonetypes_in_sector_col(fbs_collapsed)
    # check data for negative FlowAmount values
    fbs_collapsed = check_for_negative_flowamounts(fbs_collapsed)

    return fbs_collapsed


def writeFlowBySectorBibliography(methodnames):
    """
    Generate bibliography for FlowBySectorMethod in local directory
    :param methodnames: list, FBS methodnames for which to create .bib file
    :return: .bib file save to local directory
    """
    # Generate a single .bib file for a list of Flow-By-Sector method names
    # and save file to local directory
    log.info('Write bibliography to ' + biboutputpath + '_'.join(methodnames) + '.bib')
    generate_fbs_bibliography(methodnames)
