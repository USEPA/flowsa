# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

BEA data is imported as csv files from useeior and formatted into FBAs in the scripts folder
https://github.com/USEPA/flowsa/tree/master/scripts/FlowByActivity_Datasets
"""

def subset_BEA_Use(df, attr):
    """
    Function to modify loaded BEA table based on data in the FBA method yaml
    :param df: flowbyactivity dataframe
    :param attr: attribute parameters from method yaml
    :return: modified BEA dataframe
    """
    commodity = attr['clean_parameter']
    df = df.loc[df['ActivityProducedBy'] == commodity]

    # set column to None to enable generalizing activity column later
    df.loc[:, 'ActivityProducedBy'] = None

    return df
