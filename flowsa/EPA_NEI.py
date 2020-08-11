# EPA_NEI_Onroad.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls EPA National Emissions Inventory (NEI) data for nonpoint sources
"""
import pandas as pd
import numpy as np
import zipfile
import io

def epa_nei_call(url, response_load, args):
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
                            "total emissions": "FlowAmount", 
                            "scc": "ActivityProducedBy", 
                            "fips code": "Location",
                            "emissions uom":"Unit",
                            "pollutant desc": "Description"})
    
    # add hardcoded data
    df['Class']="Emission"
    df['SourceName'] = "EPA_NEI_Onroad"
    df['LocationSystem'] = "FIPS_2017"
    df['Compartment'] = "air"
    df['Year'] = args['year']
    
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    
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
                          'data set',
                          'pollutant type(s)'])

    return df
