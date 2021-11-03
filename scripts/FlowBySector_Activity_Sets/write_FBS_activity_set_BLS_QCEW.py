# write_FBS_activity_set_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files for
land use related to BLS QCEW employment data
"""

import flowsa
from flowsa.settings import flowbysectoractivitysetspath

as_year = '2017'

if __name__ == '__main__':

    # define  fba parameters
    datasource = 'BLS_QCEW'

    # Load FBS
    df_import = flowsa.getFlowByActivity(datasource, as_year)

    # drop unused columns
    df = df_import[['ActivityProducedBy']].drop_duplicates().reset_index(drop=True)

    # rename columns
    df = df.rename(columns={"ActivityProducedBy": "name"})

    # assign column values
    df = df.assign(activity_set='activity_set_1')
    df = df.assign(note='')

    # reorder dataframe
    df = df[['activity_set', 'name', 'note']]
    df = df.sort_values(['activity_set', 'name']).reset_index(drop=True)

    # save df
    df.to_csv(flowbysectoractivitysetspath + datasource + "_asets.csv", index=False)
