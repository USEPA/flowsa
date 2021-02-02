# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
import os
from flowsa.common import outputpath, fbaoutputpath, fbsoutputpath, datapath, log
from flowsa.flowbyfunctions import collapse_fbs_sectors, filter_by_geoscale
from flowsa.datachecks import check_for_nonetypes_in_sector_col, check_for_negative_flowamounts

def getFlowByActivity(flowclass, years, datasource, geographic_level='all', file_location='local'):
    """
    Retrieves stored data in the FlowByActivity format
    :param flowclass: list, a list of`Class' of the flow. required. E.g. ['Water'] or
     ['Land', 'Other']
    :param year: list, a list of years [2015], or [2010,2011,2012]
    :param datasource: str, the code of the datasource.
    :param geographic_level: 'all', 'national', 'state', 'county'. Default is 'all'
    :param file_location: 'local' or 'remote'. Default is 'local'
    :return: a pandas DataFrame in FlowByActivity format
    """
    fbas = pd.DataFrame()
    for y in years:
        # definitions
        fba_file = datasource + "_" + str(y) + ".parquet"
        local_file_path = fbaoutputpath + fba_file
        remote_file_path = 'https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowByActivity/' + fba_file
        # load data
        if file_location == 'local':
            fba = load_file(fba_file, local_file_path, remote_file_path)
        else:
            log.info('Loading ' + datasource + ' from remote server')
            fba = pd.read_parquet(remote_file_path)
        fba = fba[fba['Class'].isin(flowclass)]
        # if geographic level specified, only load rows in geo level
        if geographic_level != 'all':
            fba = filter_by_geoscale(fba, geographic_level)
        # concat dfs
        fbas = pd.concat([fbas, fba], sort=False)
    return fbas


def getFlowBySector(methodname, file_location='local'):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :param file_location: 'local' or 'remote'. Default is 'local'
    :return: dataframe in flow by sector format
    """

    # define fbs file
    fbs_file = methodname + ".parquet"
    local_file_path = fbsoutputpath + fbs_file
    remote_file_path = 'https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowBySector/' + fbs_file

    if file_location == 'local':
        fbs = load_file(fbs_file, local_file_path, remote_file_path)
    else:
        log.info('Loading ' + methodname + ' from remote server')
        fbs = pd.read_parquet(remote_file_path)

    return fbs


def load_file(datafile, local_file, remote_file):
    """
    Loads a preprocessed file
    :param datafile: a data file name with any preceeding relative file
    :param paths: instance of class Paths
    :return: a pandas dataframe of the datafile
    """
    if os.path.exists(local_file):
        log.info('Loading ' + datafile + ' from local repository')
        df = pd.read_parquet(local_file)
    else:
        try:
            log.info(datafile + ' not found in local folder; loading from remote server...')
            df = pd.read_parquet(remote_file)
        except FileNotFoundError:
            log.error("No file found for " + datafile)
    return df


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
