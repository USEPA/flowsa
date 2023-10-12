# write_Crosswalk_BEA_2012.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking BEA to NAICS for 2012 for any level

"""
from flowsa.common import load_crosswalk
from flowsa.settings import datapath


def write_BEA_crosswalk(level='Detail'):
    cw_load = load_crosswalk('NAICS_to_BEA_Crosswalk_2012')
    cw = cw_load[[f'BEA_2012_{level}_Code',
                  'NAICS_2012_Code']].drop_duplicates().reset_index(drop=True)
    # drop all rows with naics >6
    cw = cw[cw['NAICS_2012_Code'].apply(lambda x: len(str(x)) == 6)].reset_index(drop=True)

    df = cw.rename(columns={"NAICS_2012_Code": "Sector",
                            f"BEA_2012_{level}_Code":"Activity"})
    df['SectorSourceName'] = 'NAICS_2012_Code'
    df['ActivitySourceName'] = f'BEA_2012_{level}_Code'
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = df.sort_values(['Sector', 'Activity'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    # set order
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # save as csv
    df.to_csv(datapath / "activitytosectormapping" /
              f"NAICS_Crosswalk_BEA_2012_{level}.csv", index=False)

if __name__ == '__main__':
    write_BEA_crosswalk('Detail')
    write_BEA_crosswalk('Summary')
