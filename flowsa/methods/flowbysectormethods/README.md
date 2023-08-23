# FlowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls. All values are
strings unless noted.

## Special notation
Flowsa FBA and FBS method files include custom yaml configurations as defined
in `flowsa_yaml.py` using the custom `FlowsaLoader` class.

1. Include nodes from other method files using `!include` using the following 
syntax: `!include:{method.yaml}:{node1}:{node2}`
```
!include:CAP_HAP_common.yaml:source_names:EPA_NEI_Nonpoint
```

2. Incorporate a list of items (e.g., activities for an activity_set) from csv
into a method file using the following syntax: `!from_index:{file_name}.csv`
```
activity_sets:
  direct_allocation:
    selection_fields:
      PrimaryActivity: !from_index:NEI_Nonpoint_asets.csv direct_allocation
    attribution_method: direct
```
3. Call on specific functions as a parameter using the following syntax:
`!script_function:{data_source_script file} {fxn name}`
```
clean_fba_before_activity_sets: !script_function:EPA_NEI clean_NEI_fba
```

## Terms
### Target FBS output specifications
- _industry_spec_: specify the `default` desired sector aggregation:
  (`NAICS_2`, `NAICS_3`, `NAICS_4`, `NAICS_5`, `NAICS_6`). Further 
  disaggregation is possible, e.g.,:

```
industry_spec:
  default: NAICS_3
  NAICS_4: ['221', '336', '541']
```

- _year_: specify the target year of the FBS
- _target_naics_year_: specify NAICS version `2007`, `2012`, `2017`.
  At this time, only NAICS_2012_Code is supported.
- _geoscale_: level of geographic aggregation in output parquet
  (`national`, `state`, or `county`).


### Source specifications
All sources are treated recursively. That is, there can be an unlimited number
of embedded sources. The source parameters below can be applied to sources at
any level, and are inherited from higher-level sources.

- _source_names_: The name of the dataset (FBA or FBS) to serve as primary data
- _year_: year of dataset (`2015`)
- _geoscale_: level of geographic aggregation in output parquet
  (`national`, `state`, or `county`).
- _data_format_: default is `FBA`, specify `FBS` or `FBS_outside_flowsa`.
  `FBS_outside_flowsa` requires a second parameter, `FBS_datapull_fxn` which 
  supplies the name of the function to generate the FBS using the`!
  script_function:` tag.
- _activity_sets_: A subset of the FBA dataset and the method and
  allocation datasets used to create an FBS.
- _selection_fields_: A dictionary that allows subsetting source data by column.
  See description in `flowby.select_by_fields()`. To use a list of data 
  points not supplied in the method, use the `!from_index:` tag, then give 
  the name (e.g., `activity_set_1`) of the activity set as found in the csv file.
- _exclusion_fields_: A dictionary that allows subsetting source data by column.
  See description in `flowby.select_by_fields()`. 
- _attribution_method_: currently written for `direct`, `proportional`, 
  `multiplication`, `equal`, `inheritance`. See "Method Descriptions" below.
- _attribution_source_: The data source used to attribute the primary data 
  source.
   By default attribution is performed on the primary activity column.

#### Optional cleaning functions
These parameters assign functions for additional processing of FlowBy objects.
They are called using the `!script_function:` tag.
Some functions allow for extra named parameters.

- _clean_fba_before_activity_sets_: applied prior to splitting a data source
  into activity sets.
- _clean_fba_before_mapping_: applied prior to flow mapping.
- _estimate_supressed_:
- _clean_fba_: applied prior to sector columns are added.
- _clean_fba_w_sec_: applied after sector columns are added but prior to 
  attributing data to sectors.
- _clean_fba_after_attribution_: applied after activities are attributed to 
  sectors, but before applying any additional attribution methods
- _clean_fbs_: applied prior to attributing data to sectors for a FBS.

##### Defined cleaning functions
- _attribute_national_to_states()_: Propogates national data to all states 
  to enable for use in state methods. Allocates sectors across states based 
  on employment.
- _calculate_flow_per_employee()_: Calculates FlowAmount per employee per 
  year based on dataset name passed in "clean_parameter"
- _estimate_suppressed_sectors_equal_attribution()_: Equally attribute 
  known parent values to child values based on sector-length.
- _substitute_nonexistent_values()_: Fill missing values with data from 
  another geoscale. See Water_national_2015_m1 for an example.
- _weighted_average()_: Determine the weighted average of provided values. 
  See Water_national_2015_m1 for an example. 

#### Additional optional parameters
- _activity_to_sector_mapping_: name of activity to sector
  mapping file, if not provided will use the source name
- _apply_urban_rural_: (bool) Assign flow quantities as urban or rural based on
  population density by FIPS.
- _fedefl_mapping_: name of mapping file in FEDEFL. If not
  supplied will use the source name
- _mfl_mapping_: name of mapping file for Material Flow List. Should not be
  used if fedefl_mapping is used
- _keep_unmapped_rows_: (bool) default is False, if True will maintain any
  flows not found in mapping files.
- _attribute_on_: (list) specify which columns in the primary dataset 
  should be used for attribution. See REI_waste_national_2012.yaml for an 
  example. 
- _fill_columns_: (str) indicate if there is a column in the primary 
  dataset that should be filled with the values in the attribution data 
  source. See REI_waste_national_2012.yaml for an example. 


## Method Descriptions
- direct: Activities are directly assigned to sectors using the source to
  NAICS crosswalk
- multiplication: Multiply the values in the primary source with
  values sharing the same sectors in the attribution source
- proportional: Activities are proportionally attributed to sectors using
  specified attribution data source
- equal: Equally attribute parent values to child values until reach target 
  sector length
- inheritance: Assign parent values to all child values. Usefull in 
  situations where value is a rate, such as kg/m2.
