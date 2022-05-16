# Flow-By-Sector Activity Sets
Flow-By-Sector (FBS) activity sets are an optional method of assigning 
Flow-By-Actiivty (FBA) activities to an activity set defined in an FBS 
method yaml. Activity set csv files are generally created in the 
[scripts directory](https://github.com/USEPA/flowsa/tree/master/scripts/FlowBySector_Activity_Sets).
These csvs are not required, but are recommended when an FBA has a large 
number of activities.

The CSVs are called on in the FBS yaml under the `name:` parameter, using 
the tag `!from_index:CSVName.csv ActivitySetColumnSubset`. Where 
_CSVName.csv_ is the name of the activity set file and 
_ActivitySetColumnSubset_ is the value in the "activity_set" column to call 
on. See the example below.

```
"EPA_NEI_Onroad":
    data_format: 'FBA'
    class: Chemicals
    geoscale_to_use: national
    year: 2017
    activity_to_sector_mapping: 'SCC'
    clean_fba_df_fxn: !script_function:EPA_NEI clean_NEI_fba
    fedefl_mapping: 'NEI'
    activity_sets:
      direct_allocation:
        names: !from_index:NEI_Onroad_2017_asets.csv direct_allocation
```
