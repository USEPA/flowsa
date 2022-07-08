# FlowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls. All values are
strings unless noted.

## Terms
### Target FBS output specifications
- _target_sector_level_: specify desired sector aggregation (NAICS_2,
  NAICS_3, NAICS_4, NAICS_5, NAICS_6)
- _target_sector_source_: specify NAICS version 2007, 2012, 2017 (ex.
  NAICS_2012_Code). At this time, only NAICS_2012_Code is supported.
- _target_geoscale_: level of geographic aggregation in output parquet
  (national, state, or county)
- _download_if_missing_: (optional) Add and set to 'True' if you would like
  to download all required FBAs from Data Commons rather than generating
  FBAs locally.

### Source specifications (in FBA format)
- _source_names_: The name of the FBS dataset or the FBA dataset requiring
  allocation to sectors
- _data_format_: 'FBA', 'FBS', 'FBS_outside_flowsa', loads a FlowByActivity
  or a FlowBySector parquet stored in flowsa, or calls on a specified
  function to load data from outside flowsa in FBS format
- _class_: a text string in 'Class' column of flowbyactivity (ex. Water),
  see class types in
  [source_catalog.yaml](https://github.com/USEPA/flowsa/blob/master/flowsa/data/source_catalog.yaml)
- _geoscale_to_use_: the geoscale of the FBA set to use for sector allocation
   (national, state, or county)
- _year_: year of available dataset (ex. 2015)
- _activity_to_sector_mapping_: (optional) name of activity to sector
  mapping file, if not provided will use the source name
- _apply_urban_rural_: Assign flow quantities as urban or rural based on
  population density by FIPS.
- _clean_fba_before_mapping_df_fxn_: (optional) calls on function in the
  source.py file to clean up/modify the FBA data prior to mapping flows.
  Function is called using the `!script_function:` tag.
- _clean_fba_df_fxn_: (optional) calls on function in the source.py file to
  clean up/modify the FBA data prior to allocating data to sectors.
  Function is called using the `!script_function:` tag.
- _clean_fba_w_sec_df_fxn_: (optional) calls on function in the source.py
  file to clean up/modify the FBA dataframe, after sector columns are added
  but prior to allocating data to sectors. Function is called using
  the`!script_function:` tag.
- _fedefl_mapping_: (optional) name of mapping file in FEDEFL. If not
  supplied will use the source_names
- _mfl_mapping_: (optional, should not be used if fedefl_mapping is used)
  name of mapping file for Material Flow List.
- _keep_unmapped_rows_: (optional) default is False, if True will maintain any
  flows not found in mapping files.

### Activity set specifications
- _activity_sets_: A subset of the FBA dataset and the method and
  allocation datasets used to create an FBS
- _names_: (list) specify the subset of the FBA to allocate based on values in the
   Activity Produced/Consumed By fields. To use an external activity set .
  csv file, use the tag `!from_index:file_name.csv`, then give the name (e.g.,
  `activity_set_1`) of the activity set as found in the csv file.
- _source_flows_: (list, optional) specify the 'FlowName'(s) from the FBA
   to use. If not provided, all flows are used.
- _allocation_method_: currently written for 'direct',
   'allocation_function', 'proportional', and 'proportional-flagged'. See
  descriptions below.
- _allocation_source_: The primary data source used to allocate main FBA for
   specified activity to sectors
- _literature_sources_: (optional) Specific functions that contain values
  from literature used to modify FBA data.
- _activity_to_sector_mapping_: (optional) name of activity to sector
  mapping file, if not provided will use the source name
- _allocation_source_class_: specific 'FlowClass' found in the allocation
  source flowbyactivity parquet
- _allocation_source_year_: specific to the allocation datasets, use the
  year relevant to the main FBA dataframe
- _allocation_flow_: (list) the relevant 'FlowName' values, as found in the
  source flowbyactivity parquet. Use 'None' to capture all flows.
- _allocation_compartment_: (list) the relevant 'Compartment' values, as
  found in the source flowbyactivity parquet. Use 'None' to capture all
  compartments.
- _allocation_from_scale_: national, state, or county - dependent on
  allocation source, as not every level exits for sources
- _allocation_fba_load_scale_: (optional) Can indicate geographic level of
  FBA to load, helpful when an FBA ia large
- _clean_allocation_fba_: (optional) Function to clean up the allocation
  FBA, as defined in the source.py file. Function is called using
  the`!script_function:` tag.
- _clean_allocation_fba_w_sec_: (optional) Function to clean up the
  allocation FBA, after allocation activities are assigned SectorProducedBy
  and SectorConsumedBy columns. Function is called using
  the`!script_function:` tag.
- _allocation_map_to_flow_list_: (optional) If the allocation df and source
  df need to be matched on Context and/or Flowable, set to 'True'
- _helper_source_: (optional) secondary df for sector allocation
- _helper_method_: currently written for 'multiplication', 'proportional',
  and 'proportional-flagged'
- _helper_activity_to_sector_mapping_: (optional) name of activity to
  sector mapping file, if not provided will use the source name
- _helper_source_class_: specific 'FlowClass' found in the allocation
  source flowbyactivity parquet
- _helper_source_year_: specific to the allocation datasets, use the year
  relevant to the main FBA dataframe
- _helper_flow_: (list) the relevant 'FlowName' values, as found in the
  source flowbyactivity parquet
- _helper_from_scale_: national, state, or county - dependent on allocation
  source, as not every level exits for sources
- _clean_helper_fba_: (optional) Function to clean up the helper FBA.
  Function is called using the`!script_function:` tag.
- _clean_helper_fba_wsec_: (optional) Function to clean up the helper FBA,
  after allocation activities are assigned SectorProducedBy and
  SectorConsumedBy columns. Function is called using
  the`!script_function:` tag.

### Source specifications (in FBS format)
If source data format is specified as 'FBS':
- _source_names_: The name of the FBS dataset
- _data_format_: 'FBS', loads a FlowBySector
- _year_: year of available dataset (ex. 2015)
- _clean_fbs_df_fxn_: (optional) apply function to clean the FBS after it
  is accessed. Function is called using the`!script_function:` tag.

### FBS_outside_flows specifications
If source data_format is specified as `FBS_outside_flowsa`:
- _FBS_datapull_fxn_: name of the function to generate the FBS. Function is
  called using the`!script_function:` tag.
- _parameters_: (list) parameters to pass into the function

## Method Descriptions
- allocation_function: Activities are assigned to sectors using a specified
  function
- direct: Activities are directly assigned to sectors using the source to
  NAICS crosswalk
- multiplication: Multiply the values in the allocation data source with
  values sharing the same sectors in the helper allocation data source
- proportional: Activities are proportionally allocated to sectors using
  specified allocation data source
- proportional-flagged: Activities that are flagged (assigned a value of
  '1') are proportionally allocated to sectors using a specified allocation
  data source. Activities that are not flagged (assigned a value of '0')
  are directly assigned to sectors.
