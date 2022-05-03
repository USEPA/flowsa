# FlowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls. All values are strings unless noted.

## Terms
### Target FBS output specifications
1. _target_sector_level_: specify desired sector aggregation
   (NAICS_2, NAICS_3, NAICS_4, NAICS_5, NAICS_6)
2. _target_sector_source_: specify NAICS version 2007, 2012, 2017 (ex. NAICS_2012_Code).
   Recommend NAICS_2012_Code, as the majority of datasets use this version of NAICS
3. _target_geoscale_: level of geographic aggregation in output parquet (national, state, or county)
4. _download_if_missing_: (optional) Add and set to 'True' if you would like to download all required
   FBAs from Data Commons rather than generating FBAs locally.

### Source specifications (in FBA format)
1. _source_names_: The name of the FBS dataset or the FBA dataset requiring allocation to sectors
2. _data_format_: 'FBA', 'FBS', 'FBS_outside_flowsa', loads a FlowByActivity or a FlowBySector
   parquet stored in flowsa, or calls on a specified function to load data from outside flowsa in FBS format
3. _class_: a text string in 'Class' column of flowbyactivity (ex. Water), see class types in
   (source_catalog.yaml)[https://github.com/USEPA/flowsa/blob/master/flowsa/data/source_catalog.yaml]
4. _geoscale_to_use_: the geoscale of the FBA set to use for sector allocation
   (national, state, or county)
5. _year_: year of available dataset (ex. 2015)
6. _activity_to_sector_mapping_: (optional) name of activity to sector mapping file, if not provided will use the source name
7. _apply_urban_rural_: Assign flow quantities as urban or rural based on population density by FIPS.
8. _clean_fba_before_mapping_df_fxn_: (optional) calls on function in the source.py file to clean up/modify
   the FBA data prior to mapping flows.
9. _clean_fba_df_fxn_: (optional) calls on function in the source.py file to clean up/modify
   the FBA data prior to allocating data to sectors.
10. _clean_fba_w_sec_df_fxn_: (optional) calls on function in the source.py file to clean up/modify the
   FBA dataframe, after sector columns are added but prior to allocating data to sectors.
11. _fedefl_mapping_: (optional) name of mapping file in FEDEFL. If not supplied will use
   the source_names
12. _mfl_mapping_: (optional, should not be used if fedefl_mapping is used) name of mapping file for Material Flow List.

### Activity set specifications
1. _activity_sets_: A subset of the FBA dataset and the method and allocation datasets used to create a FBS
2. _names_: (list) specify the subset of the FBA to allocate based on values in the
   Activity Produced/Consumed By fields. To use an external activity set .csv file, use the tag `!from_index:file_name.csv`, then give the name (e.g. `activity_set_1`) of the activity set as found in the csv file.
3. _source_flows_: (list, optional) specify the 'FlowName'(s) from the FBA to use.
    If not provided, all flows are used.
4. _allocation_method_: currently written for 'direct', 'allocation_function',
   'proportional', and 'proportional-flagged'. See descriptions below.
5. _allocation_source_: The primary data source used to allocate main FBA for
   specified activity to sectors
6. _literature_sources_: (optional)
7. _activity_to_sector_mapping_: (optional) name of activity to sector mapping file, if not provided will use the source name
8. _allocation_source_class_: specific 'FlowClass' found in the allocation source
   flowbyactivity parquet
9. _allocation_source_year_: specific to the allocation datasets, use the year relevant
   to the main FBA dataframe
10. _allocation_flow_: (list) the relevant 'FlowName' values, as found in the source
   flowbyactivity parquet. Use 'None' to capture all flows.
11. _allocation_compartment_: (list) the relevant 'Compartment' values, as found in the source
   flowbyactivity parquet. Use 'None' to capture all compartments.
12. _allocation_from_scale_: national, state, or county - dependent on allocation source,
   as not every level exits for sources
13. _allocation_fba_load_scale_: (optional) Can indicate geographic level of FBA to load,
    helpful when an FBA ia large
14. _clean_allocation_fba_: (optional) Function to clean up the allocation FBA, as defined in
    the source.py file
15. _clean_allocation_fba_w_sec_: (optional) Function to clean up the allocation FBA, after
    allocation activities are assigned SectorProducedBy and SectorConsumedBy columns
16. _allocation_map_to_flow_list_: (optional) If the allocation df and source df need to be matched
    on Context and/or Flowable, set to 'True'
17. _helper_source_: (optional) secondary df for sector allocation
18. _helper_method_: currently written for 'multiplication', 'proportional', and 'proportional-flagged'
19. _helper_activity_to_sector_mapping_: (optional) name of activity to sector mapping file, if not provided will use the source name
20. _helper_source_class_: specific 'FlowClass' found in the allocation source
    flowbyactivity parquet
21. _helper_source_year_: specific to the allocation datasets, use the year relevant
    to the main FBA dataframe
22. _helper_flow_: (list) the relevant 'FlowName' values, as found in the source
    flowbyactivity parquet
23. _helper_from_scale_: national, state, or county - dependent on allocation source,
    as not every level exits for sources
24. _clean_helper_fba_: (optional) Function to clean up the helper FBA, as defined in
    the source.py file
25. _clean_helper_fba_wsec_: (optional) Function to clean up the helper FBA, after
    allocation activities are assigned SectorProducedBy and SectorConsumedBy columns

### Source specifications (in FBS format)
If source data format is specified as 'FBS':
1. _source_names_: The name of the FBS dataset
2. _data_format_: 'FBS', loads a FlowBySector
3. _year_: year of available dataset (ex. 2015)
4. _clean_fbs_df_fxn_: (optional) apply function to clean the FBS after it is accessed
5. _clean_fbs_df_fxn_source: (if clean_fbs_df_fxn is used) identifies the location of the function

### FBS_outside_flows specifications
If source data_format is specified as 'FBS_outside_flowsa':
1. _FBS_datapull_fxn_: name of the function to generate the FBS
2. _parameters_: (list) parameters to pass into the function

## Allocation Method Descriptions
1. direct: Activities are directly assigned to sectors using the source to NAICS crosswalk
2. allocation_function: Activities are assigned to sectors using a specified function
3. proportional: Activities are proportionally allocated to sectors using specified allocation data source
4. proportional-flagged: Activities that are flagged (assigned a value of '1') are proportionally allocated
   to sectors using a specified allocation data source. Activities that are not flagged
   (assigned a value of '0') are directly assigned to sectors.

## Helper Method
1. multiplication: Multiply the values in the allocation data source with values sharing the same sectors
   in the helper allocation data source
2. proportional: Data in allocation source further allocated to sectors proportionally with the helper source
3. proportional-flagged: Data in allocation source further allocated to sectors proportionally
   when flagged (assigned a value of '1') and directly assigned to sector when not flagged
   (assigned a value of '0')
