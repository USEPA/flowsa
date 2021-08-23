# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public API for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

from esupy.processed_data_mgmt import load_preprocessed_output
from flowsa.common import paths, biboutputpath, fbaoutputpath, fbsoutputpath,\
    DEFAULT_DOWNLOAD_IF_MISSING, log
from flowsa.metadata import set_fb_meta
from flowsa.flowbyfunctions import collapse_fbs_sectors, filter_by_geoscale
from flowsa.validation import check_for_nonetypes_in_sector_col, check_for_negative_flowamounts
import flowsa.flowbyactivity
import flowsa.flowbysector
from flowsa.bibliography import generate_fbs_bibliography


def getFlowByActivity(datasource, year, flowclass=None, geographic_level=None,
                      download_if_missing=DEFAULT_DOWNLOAD_IF_MISSING):
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
        log.info('%s %s not found in %s, downloading from remote source',
                 datasource, str(year), fbaoutputpath)
        download_from_remote(fba_meta,paths)
        fba = load_preprocessed_output(fba_meta,paths)

    if fba is None:
        log.info('%s %s not found in %s, running functions to generate FBA',
                 datasource, str(year), fbaoutputpath)
        # Generate the fba
        flowsa.flowbyactivity.main(year=year, source=datasource)
        # Now load the fba
        fba = load_preprocessed_output(fba_meta, paths)
        if fba is None:
            log.error('getFlowByActivity failed, FBA not found')
        else:
            log.info('Loaded %s %s from %s',
                     datasource, str(year), fbaoutputpath)
    else:
        log.info('Loaded %s %s from %s', datasource, str(year), fbaoutputpath)

    # Address optional parameters
    if flowclass is not None:
        fba = fba[fba['Class'] == flowclass]
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)
    return fba


def getFlowBySector(methodname, download_if_missing=DEFAULT_DOWNLOAD_IF_MISSING):
    """
    Loads stored FlowBySector output or generates it if it doesn't exist, then loads
    :param methodname: string, Name of an available method for the given class
    :param download_if_missing: bool, if True will attempt to load from remote server
        prior to generating if file not found locally
    :return: dataframe in flow by sector format
    """
    from esupy.processed_data_mgmt import download_from_remote

    fbs_meta = set_fb_meta(methodname, "FlowBySector")
    fbs = load_preprocessed_output(fbs_meta, paths)

    # Remote download
    if fbs is None and download_if_missing:
        log.info('%s not found in %s, downloading from remote source',
                 methodname, fbsoutputpath)
        # download and load the FBS parquet
        subdirectory_dict = {'.log': 'Log'}
        download_from_remote(fbs_meta, paths, subdirectory_dict=subdirectory_dict)
        fbs = load_preprocessed_output(fbs_meta, paths)

    # If remote download not specified and no FBS, generate the FBS
    if fbs is None:
        log.info('%s not found in %s, running functions to generate FBS', methodname, fbsoutputpath)
        # Generate the fba
        flowsa.flowbysector.main(method=methodname)
        # Now load the fba
        fbs = load_preprocessed_output(fbs_meta, paths)
        if fbs is None:
            log.error('getFlowBySector failed, FBS not found')
        else:
            log.info('Loaded %s from %s', methodname, fbsoutputpath)
    else:
        log.info('Loaded %s from %s', methodname, fbsoutputpath)
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


def writeFlowBySectorBibliography(methodname):
    """
    Generate bibliography for FlowBySectorMethod in local directory
    :param methodname: string, FBS methodname for which to create .bib file
    :return: .bib file save to local directory
    """
    # Generate a single .bib file for a list of Flow-By-Sector method names
    # and save file to local directory
    log.info('Write bibliography to %s%s.bib', biboutputpath, methodname)
    generate_fbs_bibliography(methodname)
