# FLowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls

## Terms
### Target FBS output specifications
1. _target_sector_level_: specify desired sector aggregation, ranging from 2-6 digits
2. _target_sector_source_: specify NAICS version 2007, 2012, 2017. Recommend NAICS_2012_Code, as the majority of datasets use 
this version of NAICS
3. _target_geoscale_: national, state, or county

### Flowbyactivity specifications
1. _flowbyactivity_sources_: The FBA dataset to be allocated to sectors
2. _class_: a text string in 'Class' flowbyactivity
3. _geoscale_to_use_: national, state, or county
4. _year_: year of avialable dataset

### Activity set specifications
1. _activity_sets_: A subset of the FBA dataset and the method and allocation datasets used to create a FBS
2. _names_: specify an activity name found in USGS_NWIS_WU
3. _allocation_source_: The primary data source used used to allocate direct-water use for speciifed activity to sectors
4. _allocation_method_: currently written for 'direct' and 'proportional'
5. _allocation_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
6. _allocation_sector_aggregation_: 'agg' (aggregated) or 'disagg' (disaggregated) depending on the allocation source. Some
of the allocation dataset crosswalks contain every level of relevant sectors (ex. NAICS for 2-6 digits), so the dataset 
should remain 'agg' because if the sector levels are 'disagg', there will be double counting. Other datasets only 
contain information for the highest relevant sector level, in which case, the allocation source should be 'disagg' to
include all relevant more specific sectors (ex. USGS_WU_Coef crosswalk)
7. _allocation_source_year_: specific to the allocation datasets, use the year relevant to the USGS_NWIS_WU dataframe
8. _allocation_flow_: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet
9. _allocation_compartment_: a list of relevant 'Compartment' values, as found in the source flowbyactivity parquet
10. _allocation_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
11. _allocation_helper_: 'yes' if second dataset is needed for allocation, 'no' if not
12. _helper_source_: secondary df for sector allocation
13. _helper_method_: currently written for 'multiplication'
14. _helper_source_class_: specific 'FlowClass' found in the allocation source flowbyactivity parquet
15. _helper_sector_aggregation_: 'agg' or 'disagg'
16. _helper_source_year_: specific to the allocation datasets, use the year relevant to the USGS_NWIS_WU dataframe
17._helper_flow_: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet
18._helper_from_scale_: national, state, or county - dependent on allocation source, as not every level exits for sources
