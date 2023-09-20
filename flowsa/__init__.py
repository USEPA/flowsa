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
import pandas as pd
import flowsa.exceptions
from flowsa.common import load_yaml_dict
from flowsa.flowsa_log import log
from flowsa.settings import sourceconfigpath, flowbysectormethodpath, \
    biboutputpath, DEFAULT_DOWNLOAD_IF_MISSING
from flowsa.flowbyfunctions import collapse_fbs_sectors, filter_by_geoscale
from flowsa.validation import check_for_nonetypes_in_sector_col, \
    check_for_negative_flowamounts
# from flowsa.bibliography import generate_fbs_bibliography
from flowsa.datavisualization import FBSscatterplot
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbysector import FlowBySector


def getFlowByActivity(
        datasource,
        year,
        flowclass=None,
        geographic_level=None,
        download_FBA_if_missing=DEFAULT_DOWNLOAD_IF_MISSING
        ) -> pd.DataFrame:
    """
    Retrieves stored data in the FlowByActivity format
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str or list, a 'Class' of the flow. Optional. E.g.
    'Water' or ['Employment', 'Chemicals']
    :param geographic_level: str, a geographic level of the data.
                             Optional. E.g. 'national', 'state', 'county'.
    :param download_FBA_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: a pandas DataFrame in FlowByActivity format
    """
    fba = FlowByActivity.getFlowByActivity(
        full_name=datasource,
        config={},
        year=int(year),
        download_ok=download_FBA_if_missing
    )

    if len(fba) == 0:
        raise flowsa.exceptions.FBANotAvailableError(
            message=f"Error generating {datasource} for {str(year)}")
    if flowclass is not None:
        fba = fba.query('Class == @flowclass')
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)
    return pd.DataFrame(fba.reset_index(drop=True))


def getFlowBySector(
        methodname,
        fbsconfigpath=None,
        download_FBAs_if_missing=DEFAULT_DOWNLOAD_IF_MISSING,
        download_FBS_if_missing=DEFAULT_DOWNLOAD_IF_MISSING,
        **kwargs
        ) -> pd.DataFrame:
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
    fbs = FlowBySector.getFlowBySector(
        method=methodname,
        external_config_path=fbsconfigpath,
        download_sources_ok=download_FBAs_if_missing,
        download_fbs_ok=download_FBS_if_missing,
        **kwargs
    )
    return pd.DataFrame(fbs)


def collapse_FlowBySector(
        methodname,
        fbsconfigpath=None,
        download_FBAs_if_missing=DEFAULT_DOWNLOAD_IF_MISSING,
        download_FBS_if_missing=DEFAULT_DOWNLOAD_IF_MISSING
        ) -> pd.DataFrame:
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

# todo: will reintroduce option to create bibliography post 2.0 release
# def writeFlowBySectorBibliography(methodname):
#     """
#     Generate bibliography for FlowBySectorMethod in local directory
#     :param methodname: string, FBS methodname for which to create .bib file
#     :return: .bib file save to local directory
#     """
#     # Generate a single .bib file for a list of Flow-By-Sector method names
#     # and save file to local directory
#     log.info(f'Write bibliography to {biboutputpath / methodname}.bib')
#     generate_fbs_bibliography(methodname)


def seeAvailableFlowByModels(flowbytype, print_method=True):
    """
    Console print and return available Flow-By-Activity or Flow-By-Sector models
    :param flowbytype: 'FBA' or 'FBS'
    :param print_method: False to skip printing to console
    :return: dict or list of available models
    """

    # fb directory contents dependent on FBA or FBS
    if flowbytype == 'FBA':
        fb_dir = os.listdir(sourceconfigpath)
    elif flowbytype == 'FBS':
        fb_dir = os.listdir(flowbysectormethodpath)
    else:
        raise ValueError("flowbytype must be 'FBA' or 'FBS'")

    # list of file names (drop extension) for yaml files in flow directory
    fb_names = [os.path.splitext(f)[0 for f in fb_dir if f.endswith('.yaml')]
    
    # further reduce list of file names by excluding common and summary_target
    exclude = ["_common", "_summary_target"]
    fb_names = [f for f in fb_names if all(s not in f for s in exclude)]
    
    if flowbytype == 'FBA':
        # create empty dictionary, this will be the data format to print FBA
        data_print = {}
        # iterate over names to build dict for FBA and handling years
        for f in fb_names:
            s = load_yaml_dict(f, 'FBA')
            try:
                years = s['years']
            except KeyError:
                years = 'YAML missing information on years'
            data_print.update({f: years})
    else:
        # data format to print FBS
        data_print = fb_names

    if print_method:
        # print data in human-readable format
        pprint.pprint(data_print, width=79, compact=True)

    return data_print


# todo: revise data vis fxns for recursive method
# def generateFBSplot(method_dict, plottype, sector_length_display=None,
#                     sectors_to_include=None, plot_title=None):
#     """
#     Plot the results of FBS models. Graphic can either be a faceted
#     scatterplot or a method comparison
#     :param method_dict: dictionary, key is the label, value is the FBS
#         methodname
#     :param plottype: str, 'facet_graph' or 'method_comparison'
#     :param sector_length_display: numeric, sector length by which to
#     aggregate, default is 'None' which returns the max sector length in a
#     dataframe
#     :param sectors_to_include: list, sectors to include in output. Sectors
#     are subset by all sectors that "start with" the values in this list
#     :return: graphic displaying results of FBS models
#     """
#
#     FBSscatterplot(method_dict, plottype, sector_length_display,
#                    sectors_to_include, plot_title)
