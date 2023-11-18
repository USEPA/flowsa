# write_FBS_activity_set_EPA_REI_waste.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files for rei waste flows
"""

import flowsa
import flowsa.flowbyactivity
from flowsa.settings import flowbysectoractivitysetspath

datasource = 'EPA_REI'
year = '2012'

if __name__ == '__main__':
    df_import = flowsa.flowbyactivity.getFlowByActivity(datasource, year)

    df = (df_import[['ActivityProducedBy', 'ActivityConsumedBy',
                     'Description']]
          .query('~Description.str.contains("primaryfactors").values')
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={"ActivityProducedBy": "name",
                           "Description": 'note'})
          .assign(activity_set='waste')
          )
    # fill null activity values with ACB
    df['name'] = df['name'].fillna(df['ActivityConsumedBy'])

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}/{datasource}_waste_'
              f'{year}_asets.csv', index=False)
