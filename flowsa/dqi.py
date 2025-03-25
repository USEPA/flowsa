"""
Functions associated with data quality scoring
"""
import numpy as np
import re
import pandas as pd

from flowsa.common import load_sector_length_cw_melt


def adjust_dqi_reliability_collection_scores(df, sector_source_year):
    """
    Adjust the dqi scores for
    Data Reliability, Data Collection

    based on source naics and target naics

    Df must have 5 columns: DataReliability, DataCollection, source_naics, target_naics, SectorSourceName

    :param df:
    :return:
    """

    # assign two new columns to df, sector length for source and target naics
    cw = load_sector_length_cw_melt(sector_source_year)
    for c in ["source", "target"]:
        df = (df.merge(cw,
                       how = "left",
                      left_on = f"{c}_naics",
                      right_on = "Sector"
                      )
              .drop(columns="Sector")
              .rename(columns={"SectorLength": f"{c}Length"})
              )
    # find difference in length between source and target naics
    df = df.assign(source_to_target_diff = df['sourceLength'] - df['targetLength'])

    # Data Reliability
    # If value maps to a different sector level than what the data set provides (maps down), then change all
    # 1/2 values to 3 because no longer direct representation (Non-verified data based on a calculation).
    # Leave values alone if maps up or no change.
    df['DataReliability'] = np.where(
        (df['source_to_target_diff'] < 0) & (df['DataReliability'].isin([1, 2])),
        3,
        df['DataReliability'])

    # Data Collection
    # If NAICS level drops, NAICS4 -> NAICS6, assign a score of 5 because no longer know if % of
    # establishments/activities represented
    df['DataCollection'] = np.where(
        df['source_to_target_diff'] < 0,
        5,
        df['DataCollection'])

    return df.drop(columns=['sourceLength', 'targetLength', 'source_to_target_diff'])
