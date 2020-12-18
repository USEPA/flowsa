# write_Crosswalk_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk for BLS QCEW to NAICS 2012. Downloaded data is already provided in NAICS
"""
import pandas as pd
import numpy as np
from flowsa.common import datapath, load_sector_length_crosswalk
from scripts.common_scripts import unique_activity_names, order_crosswalk


def link_non_bls_naics_to_naics(df):
    """
    BLS has 6 digit naics that are not recognized by Census. Map bls data to naics
    :param df:
    :return:
    """

    # load sector crosswalk
    cw = load_sector_length_crosswalk()
    cw_sub = cw[['NAICS_6']].drop_duplicates()

    # subset the df to the 6 digit naics
    df_sub = df[df['Activity'].apply(lambda x: len(x) == 6)]

    # create a list of 6 digit bls activities that are not naics
    unmapped_df = pd.merge(df_sub, cw_sub, indicator=True, how='outer', left_on='Activity',
                           right_on='NAICS_6').query('_merge=="left_only"').drop('_merge', axis=1).reset_index(drop=True)
    act_list = unmapped_df['Activity'].values.tolist()

    # if in the activity list, the sector should be modified so last digit is 0

    df['Sector'] = np.where(df['Activity'].isin(act_list), df['Activity'].apply(lambda x: x[0:5]) + '0', df['Sector'])

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2002', '2010', '2011', '2012', '2015', '2017']
    # flowclass
    flowclass = ['Employment', 'Money', 'Other']
    # datasource
    datasource = 'BLS_QCEW'
    # df of unique ers activity names
    df = unique_activity_names(flowclass, years, datasource)
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # drop activity '31-33', the three digit naics will be aggregated to 2 digits
    df = df[df['Activity'] != '31-33']

    # modify the sector for activity = '31-33'
    # df.loc[df['Activity'] == '31-33', 'Sector'] = '31'
    # df = df.append(pd.DataFrame([['BLS_QCEW', '31-33', '32']],
    #                             columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    # df = df.append(pd.DataFrame([['BLS_QCEW', '31-33', '33']],
    #                             columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)

    df = link_non_bls_naics_to_naics(df)
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"
    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
