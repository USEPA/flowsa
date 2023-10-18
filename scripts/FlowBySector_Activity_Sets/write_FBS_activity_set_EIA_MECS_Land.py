# write_FBS_activity_set_EIA_MECS_Land.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files for land use
related to EIA MECS
"""

import flowsa
import flowsa.flowbyactivity
from flowsa.settings import flowbysectoractivitysetspath

datasource = 'EIA_MECS_Land'
# year = '2010'
year = '2014'

if __name__ == '__main__':
    df_import = flowsa.flowbyactivity.getFlowByActivity(datasource, year)

    df = (df_import[['ActivityConsumedBy']]
          .drop_duplicates()
          .query('ActivityConsumedBy.str.contains("-").values')
          .reset_index(drop=True)
          .rename(columns={"ActivityConsumedBy": "name"})
          .assign(activity_set='mecs_land',
                  note=''))

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}/{datasource}_{year}_asets.csv',
              index=False)
