# elciFBS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data electricitylci for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

"""


def get_elci_emissions(yaml_load):
    import electricitylci
    import electricitylci.model_config as config
    import fedelemflowlist as fl
   
    config.model_specs = config.build_model_class('ELCI_1')
    config.model_specs.include_upstream_processes = False
    config.model_specs.regional_aggregation = 'US'
    config.model_specs.include_renewable_generation = False
    config.model_specs.include_netl_water = True

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
    
    # Update SectorProducedBy
    
    
    # Assign other fields
    emissions_df['LocationSystem'] = 'BAA'
    emissions_df['FlowType'] = 'ELEMENTARY_FLOW'
    emissions_df['Class'] = 'Chemicals'
    
    
    return emissions_df



