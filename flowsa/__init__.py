# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
from flowsa.common import fbaoutputpath, fbsoutputpath, datapath, log
from flowsa.flowbyfunctions import collapse_fbs_sectors
from flowsa.datachecks import check_for_nonetypes_in_sector_col, check_for_negative_flowamounts


def getFlowByActivity(flowclass, years, datasource):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or
     ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :return: a pandas DataFrame in FlowByActivity format
    """
    fbas = pd.DataFrame()
    for y in years:
        # first try reading parquet from your local repo
        try:
            log.info('Loading ' + datasource + ' ' + str(y) +' parquet from local repository')
            fba = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet")
            fba = fba[fba['Class'].isin(flowclass)]
            fbas = pd.concat([fbas, fba], sort=False)
        except (OSError, FileNotFoundError):
            # if parquet does not exist in local repo, read file from Data Commons
            try:
                log.info(datasource + ' parquet not found in local repo, loading from Data Commons')
                fba = pd.read_parquet('https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowByActivity/' +
                                      datasource + "_" + str(y) + '.parquet')
                fba = fba[fba['Class'].isin(flowclass)]
                fbas = pd.concat([fbas, fba], sort=False)
            except FileNotFoundError:
                log.error("No parquet file found for datasource " + datasource + "and year " + str(
                    y) + " in flowsa or Data Commons")
    return fbas


def getFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = pd.DataFrame()
    # first try reading parquet from your local repo
    try:
        log.info('Loading ' + methodname +' parquet from local repository')
        fbs = pd.read_parquet(fbsoutputpath + methodname + ".parquet")
    except (OSError, FileNotFoundError):
        # if parquet does not exist in local repo, read file from Data Commons
        try:
            log.info(methodname + ' parquet not found in local repo, loading from Data Commons')
            fbs = pd.read_parquet('https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowBySector/' +
                                  methodname + ".parquet")
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + methodname + " in flowsa or Data Commons")

    return fbs


def getFlowBySector_collapsed(methodname):
    """
    Retrieves stored data in the FlowBySector format,
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """

    # load saved FBS parquet
    fbs = getFlowBySector(methodname)
    fbs_collapsed = collapse_fbs_sectors(fbs)

    # check data
    fbs_collapsed = check_for_nonetypes_in_sector_col(fbs_collapsed)
    fbs_collapsed = check_for_negative_flowamounts(fbs_collapsed)

    return fbs_collapsed
