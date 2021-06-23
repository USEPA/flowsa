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
   
    config.model_specs = config.build_model_class('ELCI_1')
    config.model_specs.include_upstream_processes = False
    config.model_specs.regional_aggregation = 'US'
    config.model_specs.include_renewable_generation = False
    config.model_specs.include_netl_water = True

    emissions = electricitylci.get_generation_process_df()
    return emissions



