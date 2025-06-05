# Source Catalog
The 'source_catalog.yaml' file should be manually updated with each new
Flow-By-Activity.

## Term descriptions
- _class_: list, classes such as "Water" found in the Flow-By-Activity
- _activity_schema_: 'None' if activities are not NAICS-based, 
  otherwise the year of the sector data (ex. NAICS_2012_Code)
- _sector_hierarchy_: 'flat' or 'parent-completeChild' or 
  'parent-incompleteChild'. Some datasets only contain information for the 
  highest relevant sector level, in which case, the dataset is marked as showing 
  aggregated sectors only (e.g., USGS_WU_Coef crosswalk) (flat).Some dataset 
  crosswalks contain every level of relevant sectors (ex. NAICS for 2-6 
  digits), that is they are fully disaggregated, containing all parent and 
  child data/relationships (parent-completeChild). The third option, 
  parent-incompleteChild represents data that contain some information for 
  parents and children, but it is a mix of what data is available (e.g., 
  EIA_MECS_Energy)
