# EPA_NEI_Onroad.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls EPA National Emissions Inventory (NEI) 2017 data for ONROAD sources
"""
import pandas as pd
import numpy as np
import zipfile
import io

# conversion factors
lb_kg = 0.4535924
USton_kg = 907.18474

def epa_nei_onroad_call(url, response_load, args):
    """
    Takes the .zip archive returned from the url call and extracts
    the individual .csv files. The .csv files are read into a dataframe and 
    concatenated into one master dataframe containing all 10 EPA regions.
    """
    z = zipfile.ZipFile(io.BytesIO(response_load.content))
    # create a list of files contained in the zip archive
    znames = z.namelist()
    # retain only those files that are in .csv format
    znames = [s for s in znames if '.csv' in s]
    # initialize the dataframe
    df = pd.DataFrame()
    # for all of the .csv data files in the .zip archive,
    # read the .csv files into a dataframe
    # and concatenate with the master dataframe
    for i in range(len(znames)):
        df = pd.concat([df, pd.read_csv(z.open(znames[i]))])
    return df

def epa_nei_onroad_parse(dataframe_list, args):
    """
    Modifies the raw data to meet the flowbyactivity criteria. 
    Renames certain column headers to match flowbyactivity format.
    Adds a few additional columns with hardcoded data.
    Deletes all unnecessary columns.
    """
    df = pd.concat(dataframe_list, sort=True)
                       	      
    # rename columns to match flowbyactivity format
    df = df.rename(columns={"pollutant code": "FlowName",
                            "pollutant type(s)": "Class", 
                            "total emissions": "FlowAmount", 
                            "scc": "ActivityProducedBy", 
                            "fips code": "Location",
                            "emissions uom":"Unit",
                            "pollutant desc": "Description"})
           
	# convert LB/TON to KG
    df['FlowAmount'] = np.where(df['Unit']=='LB', df['FlowAmount']*lb_kg, df['FlowAmount']*USton_kg)
    df['Unit'] = "KG"
    
    # add hardcoded data
    df['SourceName'] = "EPA_NEI_Onroad"
    df['LocationSystem'] = "FIPS_2017"
    df['Compartment'] = "air"
    df['Year'] = args['year']
    
    # drop remaining unused columns
    df = df.drop(columns=['epa region code',
                          'state',
                          'fips state code',
                          'tribal name',
                          'county',
                          'data category',
                          'emissions type code',
                          'sector',
                          'aetc',
                          'reporting period',
                          'emissions operating type',
                          'data set'])
    return df






