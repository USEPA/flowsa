# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Public functions for flowsa
For standard dataframe formats, see https://github.com/USEPA/flowsa/tree/master/format%20specs
"""

import pandas as pd
import numpy as np
from flowsa.common import fbaoutputpath, fbsoutputpath, datapath, log


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
    # for assigning dtypes
    fields = {'ActivityProducedBy':'str'}
    for y in years:
        try:
            fba = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet")
            fba = fba[fba['Class'].isin(flowclass)]
            fba = fba.astype(fields)
            fbas = pd.concat([fbas, fba], sort=False)
        except FileNotFoundError:
            log.error("No parquet file found for datasource " + datasource + "and year " + str(
                y) + " in flowsa")
    return fbas


def getFlowBySector(methodname):
    """
    Retrieves stored data in the FlowBySector format
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """
    fbs = pd.DataFrame()
    try:
        fbs = pd.read_parquet(fbsoutputpath + methodname + ".parquet")
    except FileNotFoundError:
        log.error("No parquet file found for datasource " + methodname + " in flowsa")
    return fbs


def getFlowBySector_collapsed(methodname):
    """
    Retrieves stored data in the FlowBySector format, collapsing the Sector Produced/Consumed By columns into a single
    column named "Sector"
    :param methodname: string, Name of an available method for the given class
    :return: dataframe in flow by sector format
    """

    # load saved FBS parquet
    fbs = getFlowBySector(methodname)

    # collapse the FBS sector columns into one column based on FlowType
    fbs.loc[:, 'Sector'] = np.where(fbs["FlowType"] == 'TECHNOSPHERE_FLOW', fbs["SectorConsumedBy"], "None")
    fbs.loc[:, 'Sector'] = np.where(fbs["FlowType"] == 'WASTE_FLOW', fbs["SectorProducedBy"], fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where((fbs["FlowType"] == 'WASTE_FLOW') & (fbs['SectorProducedBy'] == "None"),
                                    fbs["SectorConsumedBy"], fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where((fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorProducedBy'] == "None"),
                                    fbs["SectorConsumedBy"], fbs['Sector'])
    fbs.loc[:, 'Sector'] = np.where((fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'] == "None"),
                                    fbs["SectorProducedBy"], fbs['Sector'])

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # reorder df columns
    fbs_collapsed = fbs_collapsed[['Flowable', 'Class', 'Sector', 'Context', 'Location', 'LocationSystem', 'FlowAmount',
                                   'Unit', 'FlowType', 'Year', 'MeasureofSpread', 'Spread', 'DistributionType', 'Min',
                                   'Max', 'DataReliability', 'TemporalCorrelation', 'GeographicCorrelation',
                                   'TechnologicalCorrelation', 'DataCollection']]
    # sort dataframe
    fbs_collapsed = fbs_collapsed.sort_values(['Location', 'Flowable', 'Context', 'Sector']).reset_index(drop=True)

    return fbs_collapsed
