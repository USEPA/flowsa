# -*- coding: utf-8 -*-
"""
Created on Thu May  5 14:43:34 2022

@author: EBell


"""

import requests
import pandas as pd
from flowsa.settings import externaldatapath

def getAEOseriesIDs():
    """
    Generates a crosswalk of U.S. EIA Annual Energy Outlook (AEO) series IDs,
    based on a list of category IDs corresponding to Tables in the AEO.
    """
    
    api_key = 'YOUR_API_KEY'
    
    # category IDs are based on AEO > 2022 > Reference > Energy Consumption data 
    # from the API Query Browser: https://www.eia.gov/opendata/qb.php
    table_dict = {
        'Table 2' : 4435477,
        'Table 4' : 4435487,
        'Table 5' : 4435488,
        'Table 6' : 4435489,
        'Table 7' : 4435490,
        'Table 24' : 4442186,
        'Table 25' : 4442187,
        'Table 26' : 4442188,
        'Table 27' : 4442189,
        'Table 28' : 4442190,
        'Table 29' : 4442191,
        'Table 30' : 4442192,
        'Table 31' : 4442193,
        'Table 32' : 4442194,
        'Table 33' : 4442195,
        'Table 34' : 4442196,
        }
    
    # list of units to retain
    # we are only interested in data with energy units
    keep_units = {
        'quads',
        'trillion Btu'
        }
    
    df = pd.DataFrame()
    
    for table in table_dict:
        
        print(table)
        
        # retrieve category ID
        category_ID = table_dict[table]
    
        # generate url
        category_query_url = (f'https://api.eia.gov/category/?api_key={api_key}'
               f'&category_id={category_ID}'
               f'&out=json')
        
        # retrieve data from url
        r = requests.get(category_query_url)
        json = r.json()
    
        # create dataframe from json
        df0 = pd.DataFrame(json['category']['childseries'])
    
        # drop unwanted columns
        df0 = df0.drop(['f','updated'], axis=1)
    
        # add table name
        df0['table_name'] = json['category']['name']
        
        # retain only those lines with certain units
        df0 = df0[df0['units'].isin(keep_units)]
    
        # concatenate with master dataframe    
        df = pd.concat([df,df0])
    
    # rename columns
    df.rename(columns={'name':'series_name'}, inplace=True)
    
    # remove year from series names
    df['series_id'] = df['series_id'].str.replace('2022','__year__')
    
    # reorder columns
    df = df.reindex(columns=['table_name', 'series_name', 'units', 'series_id'])
    
    # save to csv
    df.to_csv(f"{externaldatapath}/AEOseriesIDs.csv", index=False)