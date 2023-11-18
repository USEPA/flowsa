# write_FBS_activity_set_EIA_CBECS_Land.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files for land use
related EIA CBECS
"""

import flowsa
import flowsa.flowbyactivity
from flowsa.settings import flowbysectoractivitysetspath

datasource = 'EIA_CBECS_Land'
year = '2012'

if __name__ == '__main__':
    df_import = flowsa.flowbyactivity.getFlowByActivity(datasource, year)

    activities_to_drop = ['All buildings', 'Mercantile', 'Health care']

    df = (df_import[['ActivityConsumedBy']]
          .drop_duplicates()
          .query('ActivityConsumedBy not in @activities_to_drop')
          .reset_index(drop=True)
          .rename(columns={"ActivityConsumedBy": "name"})
          .assign(activity_set='cbecs_land',
                  note=''))

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}/{datasource}_{year}_asets.csv',
              index=False)
