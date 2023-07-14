# Source Catalog
The 'source_catalog.yaml' file should be manually updated with each new
Flow-By-Activity.

## Term descriptions
- _class_: list, classes such as "Water" found in the Flow-By-Activity
- _activity_schema_: 'None' if activities are not NAICS-based, 
  otherwise the year of the sector data (ex. NAICS_2012_Code)
- _sector_hierarchy_: 'parent' or 'parent-completeChild' or 
  'parent-incompleteChild'. Some dataset crosswalks contain every level of 
  relevant sectors (ex. NAICS for 2-6 digits), that is they are fully 
  disaggregated. Other datasets only contain information for the highest 
  relevant sector level, in which case, the dataset is marked as showing 
  aggregated sectors only (e.g., USGS_WU_Coef crosswalk).