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
https://edap-ord-data-commons.s3.amazonaws.com/index.html?prefix=flowsa/

The most recent version (based on timestamp) of Flow-By-Activity and
Flow-By-Sector files are loaded when running these functions
"""
import os
import pprint
from esupy.processed_data_mgmt import load_preprocessed_output, \
    download_from_remote
from flowsa.common import load_yaml_dict
from flowsa.settings import log, sourceconfigpath, flowbysectormethodpath, \
    paths, fbaoutputpath, fbsoutputpath, \
    biboutputpath, DEFAULT_DOWNLOAD_IF_MISSING
from flowsa.metadata import set_fb_meta
from flowsa.flowbyfunctions import collapse_fbs_sectors, filter_by_geoscale
from flowsa.validation import check_for_nonetypes_in_sector_col, \
    check_for_negative_flowamounts
import flowsa.flowbyactivity
import flowsa.flowbysector
from flowsa.bibliography import generate_fbs_bibliography
from flowsa.datavisualization import plotFBSresults


def getFlowByActivity(datasource, year, flowclass=None, geographic_level=None,
                      download_FBA_if_missing=DEFAULT_DOWNLOAD_IF_MISSING):
    """
    Retrieves stored data in the FlowByActivity format
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str, a 'Class' of the flow. Optional. E.g. 'Water'
    :param geographic_level: str, a geographic level of the data.
                             Optional. E.g. 'national', 'state', 'county'.
    :param download_FBA_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: a pandas DataFrame in FlowByActivity format
    """
    # Set fba metadata
    name = flowsa.flowbyactivity.set_fba_name(datasource, year)
    fba_meta = set_fb_meta(name, "FlowByActivity")

    # Try to load a local version of FBA
    fba = load_preprocessed_output(fba_meta, paths)
    # If that didn't work, try to download a remote version of FBA
    if fba is None and download_FBA_if_missing:
        log.info('%s %s not found in %s, downloading from remote source',
                 datasource, str(year), fbaoutputpath)
        download_from_remote(fba_meta, paths)
        fba = load_preprocessed_output(fba_meta, paths)
    # If that didn't work or wasn't allowed, try to construct the FBA
    if fba is None:
        log.info('%s %s not found in %s, running functions to generate FBA',
                 datasource, str(year), fbaoutputpath)
        # Generate the fba
        flowsa.flowbyactivity.main(year=year, source=datasource)
        # Now load the fba
        fba = load_preprocessed_output(fba_meta, paths)
    # If none of the above worked, log an error message
    if fba is None:
        log.error('getFlowByActivity failed, FBA not found')
    # Otherwise (that is, if one of the above methods successfuly loaded the
    # FBA), log it.
    else:
        log.info('Loaded %s %s from %s', datasource, str(year), fbaoutputpath)

    # Address optional parameters
    if flowclass is not None:
        fba = fba[fba['Class'] == flowclass]
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)
    return fba


def getFlowBySector(methodname, fbsconfigpath=None,
                    download_FBAs_if_missing=DEFAULT_DOWNLOAD_IF_MISSING,
                    download_FBS_if_missing=DEFAULT_DOWNLOAD_IF_MISSING):
    """
    Loads stored FlowBySector output or generates it if it doesn't exist,
    then loads
    :param methodname: string, Name of an available method for the given class
    :param fbsconfigpath: str, path to the FBS method file if loading a file
        from outside the flowsa repository
    :param download_FBAs_if_missing: bool, if True will attempt to load FBAS
        used in generating the FBS from remote server prior to generating if
        file not found locally
    :param download_FBS_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: dataframe in flow by sector format
    """
    fbs_meta = set_fb_meta(methodname, "FlowBySector")
    # Try to load a local version of the FBS
    fbs = load_preprocessed_output(fbs_meta, paths)
    # If that didn't work, try to download a remote version of FBS
    if fbs is None and download_FBS_if_missing:
        log.info('%s not found in %s, downloading from remote source',
                 methodname, fbsoutputpath)
        # download and load the FBS parquet
        subdirectory_dict = {'.log': 'Log'}
        download_from_remote(fbs_meta, paths,
                             subdirectory_dict=subdirectory_dict)
        fbs = load_preprocessed_output(fbs_meta, paths)
    # If that didn't work or wasn't allowed, try to construct the FBS
    if fbs is None:
        log.info('%s not found in %s, running functions to generate FBS',
                 methodname, fbsoutputpath)
        # Generate the fbs, with option to download any required FBAs from
        # Data Commons
        flowsa.flowbysector.main(
            method=methodname,
            fbsconfigpath=fbsconfigpath,
            download_FBAs_if_missing=download_FBAs_if_missing
        )
        # Now load the fbs
        fbs = load_preprocessed_output(fbs_meta, paths)
    # If none of the above worked, log an error message
    if fbs is None:
        log.error('getFlowBySector failed, FBS not found')
    # Otherwise (that is, if one of the above methods successfuly loaded the
    # FBS), log it.
    else:
        log.info('Loaded %s from %s', methodname, fbsoutputpath)
    return fbs


def collapse_FlowBySector(methodname, fbsconfigpath=None,
                          download_FBAs_if_missing=DEFAULT_DOWNLOAD_IF_MISSING,
                          download_FBS_if_missing=DEFAULT_DOWNLOAD_IF_MISSING):
    """
    Returns fbs with one sector column in place of two
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = flowsa.getFlowBySector(methodname, fbsconfigpath,
                                 download_FBAs_if_missing,
                                 download_FBS_if_missing)
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


def seeAvailableFlowByModels(flowbytype, print_method=True):
    """
    Return available Flow-By-Activity or Flow-By-Sector models
    :param flowbytype: 'FBA' or 'FBS'
    :param print_method: False to skip printing to console
    :return: console printout of available models
    """

    # return fba directory path dependent on FBA or FBS
    if flowbytype == 'FBA':
        fb_directory = sourceconfigpath
    else:
        fb_directory = flowbysectormethodpath

    # empty dictionary
    fb_dict = {}
    # empty df
    fb_df = []
    # run through all files and append
    for file in os.listdir(fb_directory):
        if file.endswith(".yaml"):
            # drop file extension
            f = os.path.splitext(file)[0]
            if flowbytype == 'FBA':
                s = load_yaml_dict(f, 'FBA')
                try:
                    years = s['years']
                except KeyError:
                    years = 'YAML missing information on years'
                fb_dict.update({f: years})
            # else if FBS
            else:
                fb_df.append(f)

    # determine format of data to print
    if flowbytype == 'FBA':
        data_print = fb_dict
    else:
        data_print = fb_df

    if print_method:
        # print data in human-readable format
        pprint.pprint(data_print, width=79, compact=True)
    return data_print


def generateFBSplot(method_dict, plottype, sector_length_display=None,
                   sectors_to_include=None, plot_title=None):
    """
    Plot the results of FBS models. Graphic can either be a faceted
    scatterplot or a method comparison
    :param method_dict: dictionary, key is the label, value is the FBS
        methodname
    :param plottype: str, 'facet_graph' or 'method_comparison'
    :param sector_length_display: numeric, sector length by which to
    aggregate, default is 'None' which returns the max sector length in a
    dataframe
    :param sectors_to_include: list, sectors to include in output. Sectors
    are subset by all sectors that "start with" the values in this list
    :return: graphic displaying results of FBS models
    """

    plotFBSresults(method_dict, plottype, sector_length_display,
                   sectors_to_include, plot_title)
