# write_FBS_activity_set_CNHW_Food.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Write the activity set csv called on in the Food_Waste_national_2018_m2 FBS
method. The CNHW activities are broken into two activity sets, the first
activity set is for activities that can be attributed to waste management
pathways using the EPA WFR. The second activity set is attributed to waste
management pathways using national level allocation ratios.
"""

import pandas as pd
import numpy as np
import flowsa
import flowsa.flowbyactivity
import flowsa.flowbysector
from flowsa.settings import flowbysectoractivitysetspath, crosswalkpath

methodname = 'CNHW_national_2014'

if __name__ == '__main__':
    df_import = flowsa.flowbysector.getFlowBySector(methodname)

    df = (df_import
          .query('Flowable=="Food"')
          [['SectorProducedBy']]
          .drop_duplicates()
          .reset_index(drop=True)
          .rename(columns={"SectorProducedBy": "name"})
          .assign(activity_set='', note='')
          )

    # load the wasted food report activity to sector crosswalk to identify
    # data to include in activity set 1
    wfr_fba = flowsa.flowbyactivity.getFlowByActivity('EPA_WFR', '2018')
    wfr_fba = wfr_fba[['ActivityProducedBy']].drop_duplicates()
    wfr_cw = pd.read_csv(f'{crosswalkpath}/NAICS_Crosswalk_EPA_WFR.csv')
    wfr = wfr_fba.merge(wfr_cw[['Activity', 'Sector']],
                        left_on='ActivityProducedBy',
                        right_on='Activity').drop(columns='Activity')

    # add column where sectors in wfr crosswalk are a partial match to those
    # in the cnhw. Where there are values, label the wfr activity set
    df['wfr'] = df['name'].str.extract(
        '^(' + '|'.join(wfr['Sector']) + ')')
    # assign activity sets dependent on if a sector is identified in the wfr
    df['activity_set'] = np.where(df['wfr'].isnull(), 'facts_and_figures',
                                  'wasted_food_report')

    # reorder dataframe
    df = (df[['activity_set', 'name', 'note']]
          .sort_values(['activity_set', 'name'])
          .reset_index(drop=True))

    df.to_csv(f'{flowbysectoractivitysetspath}/CNHW_Food_asets.csv',
              index=False)
