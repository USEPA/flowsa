# write_FBS_activity_set_BLM_PLS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files
for land use related to Bureau of Land Management
Public Land Statistics
"""

import numpy as np
import flowsa
import flowsa.flowbyactivity
from flowsa.settings import flowbysectoractivitysetspath

datasource = 'BLM_PLS'
year = '2012'

if __name__ == '__main__':
    df_import = flowsa.flowbyactivity.getFlowByActivity(datasource, year)

    df = (df_import[['ActivityConsumedBy']]
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={"ActivityConsumedBy": "name"}))

    df = (df.assign(activity_set=np.where(df.name.str.contains('Hardrock'),
                                          'hardrock_mining', 'general_mining'),
                    note=''))

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}/{datasource}_{year}_asets.csv',
              index=False)
