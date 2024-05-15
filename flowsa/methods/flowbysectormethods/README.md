# FlowBySector method yaml term descriptions
Description of parameters in flowbysectormethods yamls. All values are
strings unless noted.

## Recursive vs Sequential Attribution
The FBS methods are designed to handle recursive and sequential attribution 
methods. 

### Recursive Attribution Methods
Recursive attribution methods allow application of unlimited attribution 
methods on a primary data source. For example, a primary data source 
(such as `USDA_ERS_MLU`) can be _proportionally_ attributed to sectors with 
an attribution source (`USDA_CoA_Cropland`) _after_ `USDA_CoA_Cropland` is 
_proportionally_ attributed to a higher resolution of sectors with another 
attribution source `USDA_CoA_Cropland_NAICS`. To apply recursive 
attribution in the FBS method yaml, all attribution sources are written by 
being continually indented, as demonstrated bellow. The yaml is essentially 
read bottom-up, where the last attribution method listed is applied to the 
data source above until reaching the primary data source. 

```
  USDA_ERS_MLU:
    fedefl_mapping: USDA_ERS_MLU
    geoscale: state
    selection_fields:
      Class: Land
    activity_sets:
      cropland_crops:
        selection_fields:
         PrimaryActivity: 'Cropland used for crops'
        attribution_method: proportional
        attribution_source:
          USDA_CoA_Cropland:
            selection_fields:
              Class: Land
              FlowName: "AREA HARVESTED"
            attribution_method: proportional
            attribution_source:
              USDA_CoA_Cropland_NAICS:
                selection_fields:
                  Class: Land
                  FlowName: "AG LAND, CROPLAND, HARVESTED"
                attribution_method: direct
             
```

### Sequential Attribution Methods
Sequential attribution is defined by listing each attribution method to be 
applied to a data source. The first attribution method in the list is 
applied first, then the next, until all methods in the list are applied. 
For example, after loading `EIA_CBECS_Land` and subsetting into an activity 
set, the data can first be _proportionally_ attributed using 
`Employment_national_2012`. After attribution, a second 
_proportional_attribution_ method can be applied using 
`Employment_state_2012` data. 

```
  EIA_CBECS_Land: # commercial land use
    fedefl_mapping: EIA_CBECS_Land
    geoscale: national
    selection_fields:
      Class: Land
      Location: !include:Location_common.yaml:_national_location
    clean_fba: !script_function:EIA_CBECS_Land cbecs_land_fba_cleanup
    activity_sets:
      cbecs_land: # all activities in eia cbecs land crosswalk
        selection_fields:
          PrimaryActivity: !from_index:EIA_CBECS_Land_2012_asets.csv cbecs_land
        attribute:
          - attribution_method: proportional
            attribution_source:
              Employment_national_2012:
                data_format: FBS
                geoscale: national
          - attribution_method: proportional
            attribute_on: ['PrimarySector']
            fill_columns: Location
            attribution_source:
              Employment_state_2012:
                data_format: FBS
                geoscale: state

```

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
- _attribute_: (optional) include for sequential attribution. Follow with list of _attribution_method_ parameters. 
- _attribution_method_: currently written for `direct`, `proportional`, 
  `multiplication`, `division`, `equal`, `inheritance`. See "Method 
  Descriptions" below.
- _attribution_source_: The data source used to attribute the primary data 
  source. By default attribution is performed on the primary activity column.

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
- _calculate_flow_per_person()_: Calculates FlowAmount per person per 
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
- _fedefl_mapping_: name of mapping file to use in FEDEFL.
- _mfl_mapping_: name of mapping file for Material Flow List. Should not be
  used if fedefl_mapping is used
- _keep_unmapped_rows_: (bool) default is False, if True will maintain any
  flows not found in mapping files.
- _standardize_units_: (bool) default is True, if True will standardize fba
units to SI using [unit_conversion.csv](../../data/unit_conversion.csv).
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
- division: Divide valus in primary data source with values sharing the same 
  sectors in the attribution source
- proportional: Activities are proportionally attributed to sectors using
  specified attribution data source
- equal: Equally attribute parent values to child values until reach target 
  sector length
- inheritance: Assign parent values to all child values. Usefull in 
  situations where value is a rate, such as kg/m2.
