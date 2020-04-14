# write_NAICS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Grabs NAICS 2007, 2012, and 2017 codes from static URLs.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

pandas2ri.activate()

useeior = importr('useeior')

NAICS_crosswalk = useeior.getMasterCrosswalk(2012)
NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)
NAICS_crosswalk.to_csv(datapath+"NAICS_07_to_17_Crosswalk.csv", index=False)


##### below is code to pull from static excel files, but files are only at 6 digit naics and we want 2-6 digits

# import pandas as pd
# from flowsa.common import datapath, clean_str_and_capitalize
#
# # 2007 to 2012 NAICS concordance
# url_07 = "https://www.census.gov/eos/www/naics/concordances/2012_to_2007_NAICS.xls"
# # 2012 to 2017 NAICS concordance
# url_17 = "https://www.census.gov/eos/www/naics/concordances/2017_to_2012_NAICS.xlsx"
#
#
#
# if __name__ == '__main__':
#     # Read directly into a pandas df,
#     raw_df_07 = pd.read_excel(url_07)
#     raw_df_17 = pd.read_excel(url_17)
#
#     # skip the first few rows
#     NAICS_df_07 = pd.DataFrame(raw_df_07.loc[2:]).reindex()
#     NAICS_df_17 = pd.DataFrame(raw_df_17.loc[2:]).reindex()
#     # only keep first 4 columns (some columns imported as mixed nans and blank cells)
#     NAICS_df_07 = NAICS_df_07.iloc[:, : 4]
#     NAICS_df_17 = NAICS_df_17.iloc[:, : 4]
#     # Assign the column titles
#     NAICS_df_07.columns = raw_df_07.iloc[1, 0:4]
#     NAICS_df_17.columns = raw_df_17.iloc[1, 0:4]
#
#     # merge data frames
#     NAICS_df = pd.merge(NAICS_df_07, NAICS_df_17, on="2012 NAICS Code")
#
#     # only keep NAICS code columns, rename, and reorder
#     NAICS_df = NAICS_df.iloc[:, [0, 2, 4]]
#     NAICS_df = NAICS_df.rename(columns={"2012 NAICS Code": "NAICS_2012_Code",
#                                   "2007 NAICS Code": "NAICS_2007_Code",
#                                   "2017 NAICS Code": "NAICS_2017_Code"})
#     NAICS_df = NAICS_df[["NAICS_2007_Code", "NAICS_2012_Code", "NAICS_2017_Code"]]
#
#     print(NAICS_df)
#     # turn dataframe to csv
#     NAICS_df.to_csv(datapath+"NAICS_07_to17_Crosswalk.csv", index=False)
#
