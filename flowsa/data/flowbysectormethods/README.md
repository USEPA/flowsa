# FlowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls. All values are strings unless noted. 

## Terms
### Target FBS output specifications
1. _target_sector_level_: specify desired sector aggregation (NAICS_2, NAICS_3, NAICS_4, NAICS_5, NAICS_6)
2. _target_sector_source_: specify NAICS version 2007, 2012, 2017 (ex. NAICS_2012_Code). Recommend NAICS_2012_Code, as the majority of datasets use 
    this version of NAICS
3. _target_geoscale_: level of geographic aggregation in output parquet (national, state, or county)

### Source specifications (in FBA or FBS format)
1. _source_names_: The name of the FBS dataset or the FBA dataset requiring allocation to sectors
2. _data_format_: 'FBA', 'FBS', 'FBS_outside_flowsa', loads a FlowByActivity or a FlowBySector parquet stored in flowsa,
or calls on a specified function to load data from outside flowsa in FBS format
3. _class_: a text string in 'Class' column of flowbyactivity (ex. Water)
4. _geoscale_to_use_: the geoscale of the FBA set to use for sector allocation (national, state, or county)
5. _year_: year of available dataset (ex. 2015)
6. _clean_fba_df_fxn_: calls on function in the source.py file to clean up/modify the FBA data prior to allocating 
    data to sectors. If FBA does not need to be modified, this parameter should be 'None'
7. _clean_fba_w_sec_df_fxn_: calls on function in the source.py file to clean up/modify the FBA dataframe, after sector 
    columns are added but prior to allocating data to sectors. If FBA _with sectors_ does not need to be modified, this 
    parameter should be 'None'
8. _fedefl_mapping_: (optional) name of mapping file in FEDEFL. If not supplied will use the source_names
9. _activity_set_file_: (optional) name of mapping file within flowbysectormethods folder which contains list of names 
    for one or more activity_sets. If not supplied _names_ should be listed below

### Activity set specifications
1. _activity_sets_: A subset of the FBA dataset and the method and allocation datasets used to create a FBS
2. _names_: (list) specify the subset of the FBA to allocate based on values in the Activity Produced/Consumed By fields.
    Required if not provided in activity_set_file.
3. _allocation_method_: currently written for 'direct' and 'proportional'
4. _allocation_source_: The primary data source used used to allocate main FBA for speciifed activity to sectors
5. _allocation_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
6. _allocation_source_year_: specific to the allocation datasets, use the year relevant to the main FBA dataframe
7. _allocation_flow_: (list) the relevant 'FlowName' values, as found in the source flowbyactivity parquet. 
    Use 'None' to capture all flows.
8. _allocation_compartment_: (list) the relevant 'Compartment' values, as found in the source flowbyactivity parquet.
    Use 'None' to capture all compartments.
9. _allocation_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
10. _clean_allocation_fba_: (optional)
11. _clean_allocation_fba_w_sec: (optional)
12. _allocation_helper_: 'yes' if second dataset is needed for allocation, 'no' if not. If yes, supply the following parameters:
13. _helper_source_: secondary df for sector allocation
14. _helper_method_: currently written for 'multiplication'
15. _helper_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
16. _helper_source_year_: specific to the allocation datasets, use the year relevant to the main FBA dataframe
17. _helper_flow_: (list) the relevant 'FlowName' values, as found in the source flowbyactivity parquet
18. _helper_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
19. _clean_helper_fba_: (optional)
20. _clean_helper_fba_wsec_: (optional)

### FBS_outside_flows specifications
If source data_format is specified as 'FBS_outside_flowsa':
1. _FBS_datapull_fxn_: name of the function to generate the FBS
2. _parameters_: (list) parameters to pass into the function