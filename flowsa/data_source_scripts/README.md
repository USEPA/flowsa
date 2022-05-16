# Data Source Scripts
The Python files in the `data_source_scripts` folder include functions 
specific to each Flow-By-Activity (FBA) dataset. These functions are used to 
help load, call, and parse the FBAs. These files can also contain functions 
used in Flow-By-Sector generation. 

The functions in these files are called on in FBA and FBS method yamls 
using the tag `!script_function:PythonFileName FunctionName`
where _PythonFileName_ is the name of the Python file (e.g., 
"BLS_QCEW.py") and _FunctionName_ is the name of the function 
(e.g., "bls_clean_allocation_fba_w_sec").

```
target_sector_level: NAICS_6
target_sector_source: NAICS_2012_Code
target_geoscale: national
source_names:
  "BLS_QCEW":
    data_format: 'FBA'
    class: Employment
    geoscale_to_use: national
    source_fba_load_scale: national
    year: 2017
    clean_fba_df_fxn: !script_function:BLS_QCEW clean_bls_qcew_fba_for_employment_sat_table
    clean_fba_w_sec_df_fxn: !script_function:BLS_QCEW bls_clean_allocation_fba_w_sec
    activity_sets:
```
