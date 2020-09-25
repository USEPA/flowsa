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
2. _class_: a text string in 'Class' column of flowbyactivity (ex. Water)
3. _geoscale_to_use_: the geoscale of the FBA set to use for sector allocation (national, state, or county)
4. _year_: year of available dataset (ex. 2015)
5. _clean_fba_df_fxn_: calls on function in the source.py file to clean up/modify the FBA data prior to allocating 
data to sectors. If FBA does not need to be modified, this parameter should be 'None'
6. _clean_fba_w_sec_df_fxn_: calls on function in the source.py file to clean up/modify the FBA dataframe, after sector 
columns are added but prior to allocating data to sectors. If FBA _with sectors_ does not need to be modified, this 
parameter should be 'None'
7. _fedefl_mapping_: name of mapping file in FEDEFL. If none supplied will use the source_names
8. _activity_set_file_: name of mapping file within flowbysectormethods folder which contains list of names for one or more activity_sets

### Activity set specifications
1. _activity_sets_: A subset of the FBA dataset and the method and allocation datasets used to create a FBS
2. _names_: (list)specify the subset of the FBA to allocate based on values in the Activity Produced/Consumed By fields
3. _activity_sector_aggregation_:'agg' (aggregated) or 'disagg' (disaggregated) depending on the allocation source. Some
                                 of the allocation dataset crosswalks contain every level of relevant sectors (ex. NAICS for 2-6 digits), so the dataset 
                                 should remain 'agg' because if the sector levels are 'disagg', there will be double counting. Other datasets only 
                                 contain information for the highest relevant sector level, in which case, the allocation source should be 'disagg' to
                                 include all relevant more specific sectors (ex. USGS_WU_Coef crosswalk)
4. _allocation_method_: currently written for 'direct' and 'proportional'
5. _allocation_source_: The primary data source used used to allocate main FBA for speciifed activity to sectors
6. _allocation_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
7. _allocation_sector_aggregation_: 'agg' or 'disagg' (see _activity_sector_aggregation_ for explanation)
8. _allocation_source_year_: specific to the allocation datasets, use the year relevant to the main FBA dataframe
9. _allocation_flow_: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet
10. _allocation_compartment_: a list of relevant 'Compartment' values, as found in the source flowbyactivity parquet
11. _allocation_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
12. _allocation_disaggregation_fxn_: call on a function to further disaggregate a sector if necessary
13. _allocation_helper_: 'yes' if second dataset is needed for allocation, 'no' if not
14. _helper_source_: secondary df for sector allocation
15. _helper_method_: currently written for 'multiplication'
16. _helper_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
17. _helper_sector_aggregation_: 'agg' or 'disagg' (see _activity_sector_aggregation_ for explanation)
18. _helper_source_year_: specific to the allocation datasets, use the year relevant to the main FBA dataframe
19. _helper_flow_: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet
20. _helper_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
