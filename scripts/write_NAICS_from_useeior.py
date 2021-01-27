# write_NAICS_from_useeior.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
3 scripts:

A script to get NAICS names and a NAICS 2-3-4-5-6 crosswalk.

- from useeior amd store them as .csv.
- Depends on rpy2 and tzlocal as well as having R installed and useeior installed.

Loops through the source crosswalks to find any NAICS not in offical Census NAICS Code list. Adds the additional NAICS
to NAICS crosswalk.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath, load_household_sector_codes
import glob
import pandas as pd
import numpy as np
import rpy2.robjects.packages as packages
from rpy2.robjects import pandas2ri



def import_useeior_mastercrosswalk():
    """
    Load USEEIOR's MasterCrosswalk that links BEA data to NAICS
    :return:
    """
    pandas2ri.activate()
    # import the useeior package (r package)
    useeior = packages.importr('useeior')
    # load the .Rd file for
    cw = packages.data(useeior).fetch('MasterCrosswalk2012')['MasterCrosswalk2012']

    return cw

def write_naics_2012_crosswalk():
    """
    Create a NAICS 2 - 6 digit crosswalk
    :return:
    """
    from flowsa.flowbyfunctions import replace_NoneType_with_empty_cells

    # load the useeior mastercrosswalk
    cw_load = import_useeior_mastercrosswalk()

    # extract naics 2012 code column and drop duplicates and empty cells
    cw = cw_load[['NAICS_2012_Code']].drop_duplicates()
    cw = replace_NoneType_with_empty_cells(cw)
    cw = cw[cw['NAICS_2012_Code'] != '']

    # dictionary to replace housing and gov't transport sectors after subsetting by naics length
    dict_replacement = {'F0': 'F010',
                        'F01': 'F010',
                        'F0100': 'F01000',
                        'S0': 'S00201',
                        'S00': 'S00201',
                        'S002': 'S00201',
                        'S0020': 'S00201'
                        }

    # define sectors that might need to be appended
    house_4 = ['F010']
    house_6 = ['F01000']
    govt = ['S00201']

    # extract naics by length
    for i in range(2, 7):
        cw_name = 'cw_' + str(i)
        cw_col = 'NAICS_' + str(i)
        cw_col_m1 = 'NAICS_' + str(i-1)
        vars()[cw_name] = cw[cw['NAICS_2012_Code'].apply(lambda x: len(x) == i)].\
            reset_index(drop=True).rename(columns={'NAICS_2012_Code': cw_col})
        # address exceptions to naics length rule - housing and gov't sector transport
        vars()[cw_name][cw_col] = vars()[cw_name][cw_col].replace(dict_replacement)
        # add some housing/gov't transport sectors, depending on length
        if i in range(2, 4):
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(house_4, columns=[cw_col]), ignore_index=True)
        if i == 5:
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(house_6, columns=[cw_col]), ignore_index=True)
        if i in range(2, 6):
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(govt, columns=[cw_col]), ignore_index=True)
        # add columns to dfs with naics length - 1
        if i in range(3, 7):
            vars()[cw_name][cw_col_m1] = vars()[cw_name][cw_col].apply(lambda x: x[0:i-1])
            # address exceptions to naics length rule - housing and gov't sector transport
            vars()[cw_name][cw_col_m1] = vars()[cw_name][cw_col_m1].replace(dict_replacement)

    # merge dfs of various lengths
    naics_cw = cw_2.copy()
    for i in range(3, 7):
        cw_merge = 'cw_' + str(i)
        naics_cw = naics_cw.merge(vars()[cw_merge], how='outer')

    # save as csv
    naics_cw.to_csv(datapath + "NAICS_2012_Crosswalk.csv", index=False)

    return None






def update_naics_crosswalk():
    # update the useeior crosswalk with crosswalks created for flowsa datasets
    # read the csv loaded as a raw datafile
    naics = pd.read_csv(datapath + "NAICS_useeior_Crosswalk.csv")
    naics = naics[naics['NAICS_2007_Code'].notna()]
    # convert all rows to string
    naics = naics.astype(str)


    missing_naics_df_list = []
    # read in all the crosswalk csv files (ends in toNAICS.csv)
    for file_name in glob.glob(datapath + "activitytosectormapping/"+'*_toNAICS.csv'):
        # skip Statistics Canada GDP because not all sectors relevant
        if file_name != 'C:/Users/cbirney/git_projects/flowsa/flowsa/data/activitytosectormapping\Crosswalk_StatCan_GDP_toNAICS.csv':
            df = pd.read_csv(file_name, low_memory=False, dtype=str)
            # convert all rows to string
            df = df.astype(str)
            # determine sector year
            naics_year = df['SectorSourceName'].all()
            # subset dataframe so only sector
            df = df[['Sector']]
            # trim whitespace and cast as string, rename column
            df['Sector'] = df['Sector'].astype(str).str.strip()
            df = df.rename(columns={'Sector': naics_year})
            # extract sector year column from master crosswalk
            df_naics = naics[[naics_year]]
            # find any NAICS that are in source crosswalk but not in mastercrosswalk
            common = df.merge(df_naics, on=[naics_year, naics_year])
            missing_naics = df[(~df[naics_year].isin(common[naics_year]))]
            # drop sectors with '-'
            missing_naics = missing_naics[~missing_naics[naics_year].str.contains('-')]
            # append to df list
            missing_naics_df_list.append(missing_naics)
    # concat df list and drop duplications
    missing_naics_df = pd.concat(missing_naics_df_list,
                                 ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)
    missing_naics_df = missing_naics_df[missing_naics_df['NAICS_2012_Code'] != 'None']
    # sort df
    missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code'])
    missing_naics_df = missing_naics_df.reset_index(drop=True)

    # add missing naics to master naics crosswalk
    total_naics= naics.append(missing_naics_df, ignore_index=True)

    # append household codes
    household = load_household_sector_codes()
    h = household['Code'].drop_duplicates().tolist()
    for i in h:
        if (total_naics['NAICS_2012_Code'] != i).all():
            total_naics = total_naics.append({'NAICS_2007_Code': np.nan, 'NAICS_2012_Code': i, 'NAICS_2017_Code': np.nan},
                                             ignore_index =True)

    # sort df
    total_naics = total_naics.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code']).drop_duplicates()
    total_naics = total_naics[~total_naics['NAICS_2012_Code'].isin(['None', 'unknown', 'nan',
                                                                    'Unknown', np.nan])].reset_index(drop=True)

    # save as csv
    total_naics.to_csv(datapath + "NAICS_07_to_17_Crosswalk.csv", index=False)

    return None




#
#
#
#
# from flowsa.common import datapath, outputpath
#
#
#
# dst_path = outputpath + 'MasterCrosswalk2012.rda'
#
#
#
# from rpy2.robjects.packages import importr
#
# # load R package useeior
# useeior = importr('useeior')
#
# # load the NAICS crosswalk with NAICS2 - NAICS10
# NAICS_crosswalk = useeior.getNAICSCrosswalk(2012)
#
# # only keep through NAICS6, drop duplicates
# NAICS_crosswalk = NAICS_crosswalk.drop(columns=['NAICS_7', 'NAICS_8', 'NAICS_9', 'NAICS_10']).drop_duplicates()
# # save as csv
# NAICS_crosswalk.to_csv(datapath+"NAICS_2012_Crosswalk.csv", index=False)
#
#
# test = useeior.MasterCrosswalk2012
# test2 = useeior.use_data(MasterCrosswalk2012)
#
# test3 = useeior.BEAtoNAICSCrosswalk



# import pandas as pd
# import rpy2.robjects as ro
# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
#
# from rpy2.robjects.conversion import localconverter
#
# r_df = ro.DataFrame({'int_values': ro.IntVector([1,2,3]),
#                      'str_values': ro.StrVector(['abc', 'def', 'ghi'])})
#
# r_df
#
# df = ro.r(dst_path)
#
#
# rs4 = dst_path
#
# def subset_RS4(rs4, subset):
#     subset_func = rs4("""function(o, s){
#     o[s]
#     }
#     """)
#     return subset_func(rs4, subset)
#
# subset1 = r[">"](r["width"](peaks1), args.min_width)
# print(subset_RS4(peaks1, subset1))
#
#
#
#
#
# import rpy2.robjects as robjects
# import rpy2.rinterface as rinterface
# from rpy2.robjects.packages import importr
#
# lme4 = importr("useeior")
# getmethod = robjects.baseenv.get("getMethod")
#
# StrVector = robjects.StrVector
#
#
#
# readrpkg = packages.importr("readr")
# useeior = importr("useeior")
# dtools = importr("devtools")
#
#
# mtcars = useeior.data(datasets).fetch('mtcars')['mtcars']
#
# pandas2ri.activate()
# useeior.list.files(pattern='*.Rdata')
# cw = robjects.r['load'](useeior.MasterCrosswalk2012)
#
# cw = robjects.r['load'](file='extractedMarkerData.Rdata')
#
#
#
#
#
#
#
#










#
#
# with localconverter(ro.default_converter + pandas2ri.converter):
#   r_from_pd_df = ro.conversion.py2rpy(NAICS_crosswalk)
#

#
#
# from flowsa.common import datapath
# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
# import rpy2.robjects as ro
# pandas2ri.activate()
#
# useeior = importr('useeior')
#
# NAICS_crosswalk = useeior.getNAICSCrosswalk(2012)
# NAICS_crosswalk = ro.conversion.py2rpy(NAICS_crosswalk)
# NAICS_crosswalk.to_csv(datapath+"NAICS_2012_Crosswalk.csv", index=False)
#
# NAICS_names = useeior.getNAICSCodeName(2012)
# NAICS_names = ro.conversion.py2rpy(NAICS_names)
# NAICS_names.to_csv(datapath+"NAICS_2012_Names.csv", index=False)
#
#
#
#
#
#
#
#
#
# from flowsa.common import datapath
# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
# pandas2ri.activate()
#
# useeior = importr('useeior')
#
# NAICS_crosswalk = useeior.getNAICSCrosswalk(2012)
# mastercrosswalk = useeior.MasterCrosswalk2012
# # NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)
# NAICS_crosswalk.to_csv(datapath+"NAICS_2012_Crosswalk.csv", index=False)
#
# NAICS_names = useeior.getNAICSCodeName(2012)
# NAICS_names = pandas2ri.ri2py_dataframe(NAICS_names)
# NAICS_names.to_csv(datapath+"NAICS_2012_Names.csv", index=False)
#
#
#
# import rpy2.robjects as robjects
# from rpy2.robjects import r, pandas2ri
#
# pandas2ri.activate()
# df = robjects.r.load("https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.RData")
# df2 = pandas2ri.ri2py_dataframe(df)
#
#
#
# import rpy2.robjects as robjects
# df  = robjects.r['load']("https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.Rd")
#
#
# from flowsa.common import outputpath
# from rpy2.robjects import default_converter
# from rpy2.robjects import pandas2ri
# from rpy2.robjects.conversion import localconverter
#
# # use the default conversion rules to which the pandas conversion
# # is added
#
# file_path = outputpath + 'MasterCrosswalk2012.rda'
# # file_path = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.rda"
# # cw_load = robjects.r['load'](file_path)
# # cw_load = robjects.r['source'](file_path)
# cw_load = robjects.r['readRDS'](file_path)
# robjects.r['source'](file_path)
# with localconverter(default_converter + pandas2ri.converter) as cv:
#     dataf = robjects.r[file_path]
#
#
# import pyreadr
#
# result = pyreadr.read_r(file_path)
#
#
#
#
#
# import pyreadr
#
# url = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.Rd"
# dst_path = outputpath + 'MasterCrosswalk2012.rda'
# dst_path_again = pyreadr.download_file(url, dst_path)
# res = pyreadr.read_r(dst_path)
#
#
#
#
# from flowsa.common import datapath, load_household_sector_codes
# import glob
# import pandas as pd
# import numpy as np
# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
#
#
# # does not work due to issues with rpy2. Crosswalk was manually copied from useeior and added as csv (4/18/2020)
# pandas2ri.activate()
# useeior = importr('useeior')
# cw = useeior.MasterCrosswalk2012
#
#
# import numpy as np
# import pandas as pd
# from pyper import *
# import json
# r=R(use_pandas=True)
# model_rda_path = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.rda"
# r.assign("rmodel", model_rda_path)
#
#
# raw_data = '{"data":[[79],[63]]}'
# data = json.loads(raw_data)["data"]
#
# if type(data) is not np.ndarray:
#     data = dat = pd.DataFrame( np.array(data), columns = ['x'])
#
#
# r.assign("rdata", data)
# # rdata
# expr  = 'model <- readRDS(rmodel); result <- predict(model, rdata, probability=False)'
# r(expr)
# res= r.get('result')
#
#
#
#
#
# import pandas as pd
# import pyper as pr
#
# # data = pd.read_table("/home/liuwensui/Documents/data/csdata.txt", header=0)
# data = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.rda"
# r = pr.R(use_pandas=True)
# r.assign("rdata", data)
# r("summary(rdata)")
#
#
#
# import feather
# path = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.rda"
# feather.write_dataframe(df, path)
# df = feather.read_dataframe(path)
#
#
#
#
# import pyreadr
# import numpy as np
# data = pyreadr.read_r(dst_path)
# df = data[None]
# df
#
#
# import rpy2
# from rpy2.robjects.packages import importr
# useeior = importr('useeior')
#
#
# eset = useeior.ExpressionSet()
# type(eset)
# rpy2.robjects.methods.RS4
# tuple(eset.rclass)
# ('ExpressionSet',)
#
#
#
# import pyreadr
# import rpy2
# from rpy2.robjects.packages import importr
#
# url = "https://github.com/USEPA/useeior/blob/refac_mastercrosswalk/man/MasterCrosswalk2012.Rd"
# dst_path = outputpath + 'MasterCrosswalk2012.rda'
# dst_path_again = pyreadr.download_file(url, dst_path)
#
#
#
# from flowsa.common import datapath, load_household_sector_codes
# import glob
# import pandas as pd
# import numpy as np
# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
#
# useeior = importr('useeior')
# NAICS_crosswalk = pandas2ri.ri2py_dataframe(useeior.MasterCrosswalk2012)
#
#
# cw = useeior.MasterCrosswalk2012
#
#
# rpy2.robjects.methods.RS4
#
# class TestClass(ro.methods.RS4):
#     def test_method(self, b):
#         return ro.baseenv['$'](self, 'test_method')(b)
#
# s4testobj = TestClass(s4obj)
#
#
#
#
# import rpy2.robjects as ro
#
# dst_path = outputpath + 'MasterCrosswalk2012.rda'
#
# dt = pd.DataFrame()
# # To R DataFrame
# r_dt = ro.conversion.py2rpy(dst_path)
# # To pandas DataFrame
# pd_dt = ro.conversion.rpy2py(dst_path)
#
#
# with localconverter(ro.default_converter + pandas2ri.converter):
#   pd_from_r_df = ro.conversion.rpy2py(dst_path)
#
# pd_from_r_df
#
#
