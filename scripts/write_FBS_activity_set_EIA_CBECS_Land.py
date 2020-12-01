# write_FBS_activity_set_BLM_PLS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Write the csv called on in flowbysectormethods yaml files for land use related EIA CBECS
"""

import pandas as pd
import numpy as np
from flowsa.common import crosswalkpath, flowbysectoractivitysetspath


if __name__ == '__main__':
    # assign datasource
    datasource = 'EIA_CBECS_Land'

    # Read BLM PLS crosswalk
    df_import = pd.read_csv(crosswalkpath + "Crosswalk_" + datasource + "_toNAICS.csv")

    # drop unused crosswalk columns
    df = df_import[['Activity']]

    # rename columns
    df = df.rename(columns={"Activity": "name"})

    # assign column values
    # hardrock is only value in activity set 2
    df = df.assign(activity_set='activity_set_1')
    df = df.assign(note='')

    # reorder dataframe
    df = df[['activity_set', 'name', 'note']]
    df = df.sort_values(['activity_set', 'name']).drop_duplicates().reset_index(drop=True)

    # save df
    df.to_csv(flowbysectoractivitysetspath + datasource + "_asets.csv", index=False)
