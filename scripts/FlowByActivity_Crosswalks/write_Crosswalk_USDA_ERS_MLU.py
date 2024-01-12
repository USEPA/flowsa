# write_Crosswalk_UDSA_ERS_MLU.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded USDA_ERS_MLU to NAICS_12.
Created by selecting unique Activity Names and
manually assigning to NAICS

"""

import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    manually assign each ERS activity to a NAICS_2012 code

    Descriptions of activities from
    https://www.ers.usda.gov/data-products/major-land-uses/
    Accessed on: November 2, 2020
    :param df:
    :return:
    """

    # Cropland harvested, crop failure, and cultivated summer fallow.
    df.loc[df['Activity'] == 'Cropland used for crops', 'Sector'] = '111'

    # Land completely idled and lands seeded to soil
    # improvement crops but not harvested or pastured.
    df.loc[df['Activity'] == 'Cropland idled', 'Sector'] = '111'

    # Pasture considered to be in long-term crop rotation.
    # Includes some land used for pasture that could have been /
    # cropped without additional improvement.
    df.loc[df['Activity'] == 'Cropland used for pasture', 'Sector'] = '112'

    #  Farmsteads, farm roads, and lanes plus other miscellaneous farmland.
    #  (part of special uses of land) will include value in 'unaccounted' land in us
    # df.loc[df['Activity'] == 'Farmsteads, roads, and miscellaneous farmland', 'Sector'] = ''

    # Woodland grazed in farms plus estimates of forested grazing land not in farms.
    df.loc[df['Activity'] == 'Forest-use land grazed', 'Sector'] = '112'
    # Total forest-use land minus forest-use land grazed.
    df.loc[df['Activity'] == 'Forest-use land not grazed', 'Sector'] = '113'

    # Grassland and other nonforested pasture and range in
    # farms plus estimates of open or nonforested grazing
    # lands not in farms. Does not include cropland used for pasture or forest land grazed.
    df.loc[df['Activity'] == 'Grassland pasture and range', 'Sector'] = '112'

    # Land owned by Department of Defense and Department of
    # Energy and used for airfields, research and development,
    # housing, and miscellaneous military uses.
    df.loc[df['Activity'] == 'Land in defense and industrial areas', 'Sector'] = '928110'

    # Federal and State parks, wilderness areas, and wildlife refuges.
    df.loc[df['Activity'] == 'Land in rural parks and wildlife areas', 'Sector'] = '71219'

    # Highways, roads, and railroad rights-of-way, plus airport facilities outside of urban areas.
    df.loc[df['Activity'] == 'Land in rural transportation facilities', 'Sector'] = '484'
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in rural transportation facilities', '485']],
                            columns=['ActivitySourceName', 'Activity', 'Sector']
                            )], ignore_index=True, sort=True)
    # personal consumption expenditures
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in rural transportation facilities',
                              'F01000']],
                            columns=['ActivitySourceName', 'Activity', 'Sector']
                            )], ignore_index=True, sort=True)
    # state/local gov't passenger transit
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in rural transportation facilities',
                              'S00201']],
                            columns=['ActivitySourceName', 'Activity', 'Sector']
                            )], ignore_index=True, sort=True)
    # railroads
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in rural transportation facilities',
                                  '482112']],
                            columns=['ActivitySourceName', 'Activity', 'Sector']
                            )], ignore_index=True, sort=True)
    # airports
    df = pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in rural transportation facilities',
                              '488119']],
                            columns=['ActivitySourceName', 'Activity', 'Sector']
                            )], ignore_index=True, sort=True)

    # Densely-populated areas with at least 50,000 people
    # (urbanized areas) and densely-populated areas with /
    # 2,500 to 50,000 people (urban clusters).
    df.loc[df['Activity'] == 'Land in urban areas', 'Sector'] = '481'
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', '482']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', '484']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', '485']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    # personal consumption expenditures
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', 'F01000']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', '488119']],    # airports
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    # city parks/open space
    pd.concat([df,
               pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', '712190']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                )], ignore_index=True, sort=True)
    # state/local gov't passenger transit
    df = df.append(pd.DataFrame([['USDA_ERS_MLU', 'Land in urban areas', 'S00201']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                ), ignore_index=True, sort=True)

    # Unclassified uses such as marshes, swamps, bare rock,
    # deserts, tundra plus other uses not estimated, /
    # classified, or inventoried.
    # only want partial value for residential rural land
    df.loc[df['Activity'] == 'Other land',
           'Sector'] = 'F01000'

    # categories not included to prevent double counting

    # Forest-use land grazed and forest-use land not grazed.
    # df.loc[df['Activity'] == 'Forest-use land (all)', 'Sector'] = ''

    # Rural transportation, rural parks and wildlife,
    # defense and industrial, plus miscellaneous farm and other special uses.
    # df.loc[df['Activity'] == 'All special uses of land', 'Sector'] = ''

    # The sum of cropland used for crops, cropland idled, and cropland used for pasture.
    # df.loc[df['Activity'] == 'Total cropland', 'Sector'] = '11'

    # The sum of cropland, pasture/range, forest-use land, special uses, urban area, and other land.
    # df.loc[df['Activity'] == 'Total land', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2007', '2012']
    # datasource
    datasource = 'USDA_ERS_MLU'
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f"{datapath}/activitytosectormapping/"
              f"NAICS_Crosswalk_{datasource}.csv", index=False)
