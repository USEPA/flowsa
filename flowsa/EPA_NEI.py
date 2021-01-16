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
from flowsa.flowbyfunctions import assign_fips_location_system

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
        df = df.rename(columns={"pollutant desc": "FlowName",
                                "total emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "fips code": "Location",
                                "emissions uom":"Unit",
                                "pollutant code": "Description"})
    
    elif args['year'] == '2014':
        df = df.rename(columns={"pollutant_desc": "FlowName",
                                "total_emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "state_and_county_fips_code": "Location",
                                "uom":"Unit",
                                "pollutant_cd": "Description"})
    
    elif args['year'] == '2011' or args['year'] == '2008':
        df = df.rename(columns={"description": "FlowName",
                                "total_emissions": "FlowAmount", 
                                "scc": "ActivityProducedBy", 
                                "state_and_county_fips_code": "Location",
                                "uom":"Unit",
                                "pollutant_cd": "Description"})
    
    # make sure FIPS are string and 5 digits
    df['Location']=df['Location'].astype('str').apply('{:0>5}'.format)
    # remove records from certain FIPS
    excluded_fips = ['78','85','88'] 
    df = df[~df['Location'].str[0:2].isin(excluded_fips)]
    excluded_fips2 = ['777']
    df = df[~df['Location'].str[-3:].isin(excluded_fips2)]
    
    # drop all other columns
    df.drop(df.columns.difference(['FlowName',
                                   'FlowAmount',
                                   'ActivityProducedBy',
                                   'Location',
                                   'Unit',
                                   'Description']), 1, inplace=True)
    
    # add hardcoded data
    df['FlowType']="ELEMENTARY_FLOW"
    df['Class']="Chemicals"
    df['SourceName'] = args['source']
    df['Compartment'] = "air"
    df['Year'] = args['year']
    df = assign_fips_location_system(df, args['year'])
   
    return df

def epa_nei_onroad_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to ONROAD data.
    """
    df = epa_nei_global_parse(dataframe_list, args)
    
    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 1
    
    return df

def epa_nei_nonroad_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to NONROAD data.
    """
    df = epa_nei_global_parse(dataframe_list, args)
    
    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 1    
    
    return df

def epa_nei_nonpoint_parse(dataframe_list, args):
    """
    Calls global parse function to run parsing operations common 
    to all three data categories.
    Runs additional parsing operations specific to NONPOINT data.
    """
    df = epa_nei_global_parse(dataframe_list, args)

    # Add DQ scores
    df['DataReliability'] = 3
    df['DataCollection'] = 5 # data collection scores are updated in fbs as
    # a function of facility coverage from point source data
    
    return df

def assign_nonpoint_dqi(args):
    '''
    Compares facility coverage data between NEI point and Census to estimate
    facility coverage in NEI nonpoint
    '''
    import stewi
    import flowsa
    nei_facility_list = stewi.getInventoryFacilities('NEI',args['year'])
    nei_count = nei_facility_list.groupby('NAICS')['FacilityID'].count()
    census = flowsa.getFlowByActivity(flowclass=['Other'], years=[args['year']],
                                                           datasource="Census_CBP")
    census = census[census['FlowName']=='Number of establishments']
    census_count = census.groupby('ActivityProducedBy')['FlowAmount'].sum()

    #TODO compare counts across NAICS depending on granularity of fbs method

def clean_NEI_fba(fba):
    
    fba = remove_duplicate_NEI_flows(fba)
    fba = drop_GHGs(fba)
    # Remove the portion of PM10 that is PM2.5 to eliminate double counting and rename flow
    fba = remove_flow_overlap(fba, 'PM10 Primary (Filt + Cond)', ['PM2.5 Primary (Filt + Cond)'])
    fba.loc[(fba['FlowName'] == 'PM10 Primary (Filt + Cond)'), 'FlowName'] = 'PM10-PM2.5'
    return fba

def clean_NEI_fba_no_pesticides(fba):
    
    fba = drop_pesticides(fba)
    fba = clean_NEI_fba(fba)
    return fba

def remove_duplicate_NEI_flows(df):
    """ 
    These flows for PM will get mapped to the primary PM flowable in FEDEFL
    resulting in duplicate emissions
    """
    flowlist = [
                'PM10-Primary from certain diesel engines',
                'PM25-Primary from certain diesel engines',
                ]
    
    df = df.loc[~df['FlowName'].isin(flowlist)]
    return df

def drop_GHGs(df):
    """
    GHGs are included in some NEI datasets. If these data are not compiled together 
    with GHGRP, need to remove them as they will be tracked from a different source
    """
    # Flow names reflect source data prior to FEDEFL mapping
    flowlist = [
                'Carbon Dioxide',
                'Methane',
                'Nitrous Oxide',
                'Sulfur Hexafluoride',
                ]
    
    df = df.loc[~df['FlowName'].isin(flowlist)]
    
    return df    

def drop_pesticides(df):
    """
    To avoid overlap with other datasets, emissions of pesticides from pesticide
    application are removed. 
    """
    # Flow names reflect source data prior to FEDEFL mapping
    flowlist = [
                '2,4-Dichlorophenoxy Acetic Acid',
                'Captan',
                'Carbaryl',
                'Methyl Bromide',
                'Methyl Iodide',
                'Parathion',
                'Trifluralin',
                ]
    
    activity_list = [
                    '2461800001',
                    '2461800002',
                    '2461850000',
                    ]
    
    df = df.loc[~(df['FlowName'].isin(flowlist) &
                df['ActivityProducedBy'].isin(activity_list))]
    
    return df    

def remove_flow_overlap(df, aggregate_flow, contributing_flows):
    """
    Quantity of contributing flows is subtracted from aggregate flow and the
    aggregate flow quantity is updated. Modeled after function of same name in
    stewicombo.overlaphandler.py
    """
    match_conditions = ['ActivityProducedBy','Compartment','Location','Year']

    df_contributing_flows = df.loc[df['FlowName'].isin(contributing_flows)]
    df_contributing_flows = df_contributing_flows.groupby(match_conditions, as_index=False)['FlowAmount'].sum()

    df_contributing_flows['FlowName']=aggregate_flow
    df_contributing_flows['ContributingAmount'] = df_contributing_flows['FlowAmount']
    df_contributing_flows.drop(columns=['FlowAmount'], inplace=True)
    df = df.merge(df_contributing_flows, how='left', on=match_conditions.append('FlowName'))
    df[['ContributingAmount']] = df[['ContributingAmount']].fillna(value=0)
    df['FlowAmount']=df['FlowAmount']-df['ContributingAmount']
    df.drop(columns=['ContributingAmount'], inplace=True)

    # Make sure the aggregate flow is non-negative
    df.loc[((df.FlowName == aggregate_flow) & (df.FlowAmount <= 0)), "FlowAmount"] = 0    
    return df
