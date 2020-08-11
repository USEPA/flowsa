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
from flowbyfunctions import assign_fips_location_system

def epa_nei_url_helper(build_url, config, args):
    """
    Takes the basic url text and performs substitutions based on NEI year and version. 
    Returns the finished url.
    """
    urls = []
    url = build_url
    
    url = url.replace('__year__', args['year'])

    if args['year'] == '2017':
        url = url.replace('__version__', '2017v1/2017neiApr')
    elif args['year'] == '2014':
        url = url.replace('__version__', '2014v2/2014neiv2')
    elif args['year'] == '2011':
        url = url.replace('__version__', '2011v2/2011neiv2')
    elif args['year'] == '2008':
        url = url.replace('__version__', '2008neiv3')
    urls.append(url)
    return urls

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

def epa_nei_global_parse(dataframe_list, args):
    """
    Modifies the raw data to meet the flowbyactivity criteria. 
    Renames certain column headers to match flowbyactivity format.
    Adds a few additional columns with hardcoded data.
    Deletes all unnecessary columns.
    """
    df = pd.concat(dataframe_list, sort=True)
                       	      
    # rename columns to match flowbyactivity format
    if args['year'] == '2017':
        df = df.rename(columns={"pollutant code": "FlowName",
                                "total emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "fips code": "Location",
                                "emissions uom":"Unit",
                                "pollutant desc": "Description"})
    
    elif args['year'] == '2014':
        df = df.rename(columns={"pollutant_cd": "FlowName",
                                "total_emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "state_and_county_fips_code": "Location",
                                "uom":"Unit",
                                "pollutant_desc": "Description"})
    
    elif args['year'] == '2011' or args['year'] == '2008':
        df = df.rename(columns={"pollutant_cd": "FlowName",
                                "total_emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "state_and_county_fips_code": "Location",
                                "uom":"Unit",
                                "description": "Description"})
    
    # drop all other columns
    df.drop(df.columns.difference(['FlowName',
                                   'FlowAmount',
                                   'ActivityProducedBy',
                                   'Location',
                                   'Unit',
                                   'Description']), 1, inplace=True)
    
    # add hardcoded data
    df['Class']="Chemical"
    df['SourceName'] = args['source']
    df['Compartment'] = "air"
    df['Year'] = args['year']
    df = assign_fips_location_system(df, args['year'])
    
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    
    return df

def epa_nei_onroad_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to ONROAD data.
    """
    df = epa_nei_global_parse(dataframe_list, args)
    
    return df

def epa_nei_nonroad_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to NONROAD data.
    """
    df = epa_nei_global_parse(dataframe_list, args)
    
    return df

def epa_nei_nonpoint_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to NONPOINT data.
    """
    df = epa_nei_global_parse(dataframe_list, args)
    
    return df