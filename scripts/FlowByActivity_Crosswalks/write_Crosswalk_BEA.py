# write_Crosswalk_BEA.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking BEA to NAICS for any level

"""
from flowsa.common import load_crosswalk
from flowsa.settings import datapath


def write_BEA_crosswalk(level='Detail', year=2012):
    cw_load = load_crosswalk(f'Naics_to_BEA_Crosswalk_{year}')
    cw = cw_load[[f'BEA_{year}_{level}_Code',
                  f'NAICS_{year}_Code']].drop_duplicates().reset_index(drop=True)
    # drop all rows with naics >6
    cw = cw[cw[f'NAICS_{year}_Code'].apply(lambda x: len(str(x)) == 6)].reset_index(drop=True)

    df = cw.rename(columns={f"NAICS_{year}_Code": "Sector",
                            f"BEA_{year}_{level}_Code":"Activity"})
    df['SectorSourceName'] = f'NAICS_{year}_Code'
    df['ActivitySourceName'] = f'BEA_{year}_{level}_Code'
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
              f"NAICS_Crosswalk_BEA_{year}_{level}.csv", index=False)

if __name__ == '__main__':
    write_BEA_crosswalk('Detail', 2017)
    write_BEA_crosswalk('Summary', 2017)
