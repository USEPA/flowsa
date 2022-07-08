# Activity to Sector Mapping
This directory contains csv files that map activities for various datasets to
sectors. These files are not required for datasets where activities are already
NAICS-like. 

Each csv contains columns for:
1. _ActivitySourceName_: Activity Source Name must match the file name, 
   although the name can be missing extensions. For example, the Activity 
   Source Name can be "EPA_GHGI" rather than "EPA_GHGI_T_2_4", as the 
   function that looks for the file names will strip "_XXX" from the file 
   name until the file is found
2. _Activity_: Any activities that should be mapped to a sector
3. _SectorSourceName_ Specify the sector year being mapped to (e.g. 
   NAICS_2012_Code)
4. _Sector_: The 2- to 7-digit NAICS code that the activity relates to. Can 
   map to multiple NAICS codes of varying lengths. Optional: If necessary a 
   user can map to their own non-official NAICS codes. If mapped to 
   non-official NAICS, the NAICS crosswalk must be recreated in the 
   [scripts directory](https://github.com/USEPA/flowsa/blob/master/scripts/update_NAICS_crosswalk.py)
5. _SectorType_: "I" for industry, "C" for commodity
6. _Notes_: (optional) Any additional relevant information
