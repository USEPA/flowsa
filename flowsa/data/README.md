# Source Catalog
The 'source_catalog.yaml' file should be manually updated with each new
Flow-By-Activity.

## Term descriptions
- _class_: list, classes such as "Water" found in the Flow-By-Activity
- _sector-like_activities_: 'True' or 'False', “sector-like activities” are True when the Flow-By-Activity
  “ActivityProducedBy” and “ActivityConsumedBy” columns are already NAICS based. For example, all BLS QCEW
  data for employment and establishments are published by NAICS codes. We deem these “sector-like” because we
  then implement checks to determine if the activities are published in the identified NAICS year in the
  Flow-By-Sector and if not, we have a crosswalk to map the sectors/NAICS to NAICS year. 
- _activity_schema_: 'None' if 'sector-like_activities' is False, otherwise the year of the sector data
  (ex. NAICS_2012_Code)
- _sector_aggregation_level_: 'aggregated' or 'disaggregated'. Some
        dataset crosswalks contain every level of relevant sectors (ex. NAICS 
        for 2-6 digits), that is they are fully disaggregated. Other datasets only 
        contain information for the highest relevant sector level, in which case, 
        the dataset is marked as showing aggregated sectors only 
        (ex. USGS_WU_Coef crosswalk)