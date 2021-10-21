# elciFBS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data electricitylci for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

"""
import pandas as pd
from flowsa.sectormapping import get_activitytosector_mapping
from flowsa.flowbyfunctions import aggregator
from flowsa.common import US_FIPS, fbs_default_grouping_fields


def generate_elci_model_specs():
    """
    To prevent errors when loading elci files, first generate the model specs
    :return:
    """
    import electricitylci.model_config as config

    config.model_specs = config.build_model_class('ELCI_1')
    config.model_specs.include_upstream_processes = False
    config.model_specs.regional_aggregation = 'US'
    config.model_specs.include_renewable_generation = False
    config.model_specs.include_netl_water = True
    config.model_specs.stewicombo_file = 'ELCI_1'
    if 'DMR' not in config.model_specs.inventories_of_interest.keys():
        config.model_specs.inventories_of_interest['DMR'] = 2016

    return config


def get_elci_emissions(yaml_load):
    import electricitylci
    import electricitylci.model_config as config
    import fedelemflowlist as fl
   
    config.model_specs = config.build_model_class('ELCI_1')
    config.model_specs.include_upstream_processes = False
    config.model_specs.regional_aggregation = 'US'
    config.model_specs.include_renewable_generation = False
    config.model_specs.include_netl_water = True
    config.model_specs.stewicombo_file = 'ELCI_1'
    if 'DMR' not in config.model_specs.inventories_of_interest.keys():
        config.model_specs.inventories_of_interest['DMR'] = 2016

    emissions = electricitylci.get_generation_process_df()
    emissions = emissions.loc[emissions['stage_code'] == 'Power plant']
    # FlowUUID is not null (will remove RCRA flows)
    emissions = emissions.loc[~emissions['FlowUUID'].isna()]
    
    column_dict = {'Balancing Authority Name':'Location',
                   'FuelCategory':'SectorProducedBy',
                   'FlowName':'Flowable',
                   'Compartment':'Compartment',
                   'FlowUUID':'FlowUUID',
                   'FlowAmount':'FlowAmount',
                   'Unit':'Unit',
                   'Year':'Year',
                   'source_string':'MetaSources'}
    
    col_list = [c for c in column_dict.keys() if c in emissions]
    emissions_df = emissions[col_list]
    emissions_df = emissions_df.rename(columns = column_dict)
    
    # Update Compartment from FEDEFL
    fedefl = fl.get_flows()[['Flow UUID','Context']]
    emissions_df = emissions_df.merge(fedefl, how = 'left', 
                                      left_on = 'FlowUUID',
                                      right_on = 'Flow UUID')
    emissions_df.drop(columns=['Flow UUID', 'Compartment'], inplace=True)

    # Assign other fields
    emissions_df['LocationSystem'] = 'BAA'
    emissions_df['FlowType'] = 'ELEMENTARY_FLOW'
    emissions_df['Class'] = 'Chemicals'

    # Update SectorProducedBy
    mapping = get_activitytosector_mapping('eLCI')
    mapping = mapping[['Activity','Sector']]
    emissions_df_mapped = emissions_df.merge(mapping, how = 'left',
                                            left_on = 'SectorProducedBy',
                                            right_on = 'Activity')
    emissions_df_mapped.drop(columns=['SectorProducedBy','Activity'],
                             inplace=True)
    emissions_df_mapped.rename(columns={'Sector':'SectorProducedBy'},
                               inplace=True)
    emissions_df_mapped.dropna(subset=['SectorProducedBy'],
                               inplace=True)

    return emissions_df_mapped


def get_plant_water_use():
    """
    Load the 2016 plant water use from electricityLCI and format into FBS
    :return:
    """

    # first generate elci model specs
    generate_elci_model_specs()

    # after loading model specs, import water fxn
    from electricitylci.plant_water_use import generate_plant_water_use
    df_load = generate_plant_water_use(2016)

    # loaded data is a FBA with UUID
    # aggregate by location and month and set location at national level
    # df = aggregator(df_load, fbs_default_grouping_fields)
    # df['Location'] = US_FIPS





