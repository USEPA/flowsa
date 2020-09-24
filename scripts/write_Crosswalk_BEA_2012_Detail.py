# write_Crosswalk_BEA_2012_Detail.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking BEA to NAICS for 2012 Detail

"""
from flowsa.common import datapath, load_bea_crosswalk


if __name__ == '__main__':

    cw_load = load_bea_crosswalk()
    cw = cw_load[['BEA_2012_Detail_Code', 'NAICS_2012_Code']].drop_duplicates().reset_index(drop=True)
    # drop all rows with naics >6
    cw = cw[cw['NAICS_2012_Code'].apply(lambda x: len(str(x)) == 6)].reset_index(drop=True)

    df = cw.rename(columns={"NAICS_2012_Code": "Sector",
                            "BEA_2012_Detail_Code":"Activity"})
    df['SectorSourceName'] = 'NAICS_2012_Code'
    df['ActivitySourceName'] = 'BEA_2012_Detail_Code'
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = df.sort_values('Sector')
    # reset index
    df.reset_index(drop=True, inplace=True)
    # set order
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_BEA_2012_Detail_toNAICS.csv", index=False)
