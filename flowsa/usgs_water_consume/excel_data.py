# data_pull.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

import os
import requests
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile


"""
Classes and methods for pulling data from a USGS web service.

Available functions:
-
"""


def get_download_path():
    """Returns the default downloads path for linux or windows"""
    if os.name == 'nt':
        import winreg
        sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
        downloads_guid = '{374DE290-123F-4565-9164-39C4925E467B}'
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
            location = winreg.QueryValueEx(key, downloads_guid)[0]
        return location
    else:
        return os.path.join(os.path.expanduser('~'), 'downloads')


def check_and_delete_file(file_string):
    """Checks to see if specifed file exist if it does it deletes it"""
    if os.path.exists(file_string):
        os.remove(file_string)
    else:
        print("The file does not exist")


print('Begining File Download with requests')
url = 'https://www.sciencebase.gov/catalog/file/get/5af3311be4b0da30c1b245d8?f=__disk__29%2Fc0%2F51%2F29c051a5166ae254b942322f77b02edcda0822ac'

# r = requests.get(url)
path = get_download_path()
file = path + '\\usco2015v2.0.xlsx'
# check_and_delete_file(file)
# open(file, 'wb' ).write(r.content)

df = pd.read_excel(file)
headers = df.iloc[0]
new_df = pd.DataFrame(df.values[1:], columns=headers)
print("Column Names")
print(new_df.columns)

# for i in df.index:
#     print(new_df['FIPS'][i], i)

listFIPS = new_df['FIPS']
# print(listFIPS)
df_length = new_df.index.values.size
# for i in listFIPS:
for i in range(df_length):
    print(listFIPS[i])

print('End File Download with requests')
# check_and_delete_file(file)
