# Process Adjustments
Process adjustments allow for adjustments to the `SectorProducedBy` field 
for data obtained from stewicombo. Records that are from the `source_naics` 
AND the `source_process` are reassigned to the `target_naics` indicated in 
the process adjustment file.

Adjustments are indicated by identifying one or more named files in the 
`reassign_process_to_sectors` FBS parameter.


## Available Adjustments

File | Adjustment |
----- | ---- |
airplane_emissions | subtract emissions related to air transportation from airports in the NEI (https://github.com/USEPA/flowsa/issues/1)

