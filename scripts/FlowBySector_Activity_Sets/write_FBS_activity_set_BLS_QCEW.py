# write_FBS_activity_set_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the csv called on in flowbysectormethods yaml files for
land use related to BLS QCEW employment data
"""

import flowsa
from flowsa.settings import flowbysectoractivitysetspath

datasource = 'BLS_QCEW'
as_year = '2017'

if __name__ == '__main__':
    df_import = flowsa.getFlowByActivity(datasource, as_year)

    df = (df_import[['ActivityProducedBy']]
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={"ActivityProducedBy": "name"})
          .assign(activity_set='qcew',
                  note=''))

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}{datasource}_asets.csv',
              index=False)
