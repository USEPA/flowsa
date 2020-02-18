# Flow-By-Activity Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Class | String | Y | Class of the flow. See [Flow Classes](./README.md#FlowClasses).   
SourceName | String | Y | ID or name of flow in its native source
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy.
Unit | String | Y | SI unit acronym. `kg` for mass flows; `MJ` for energy flows
Activity | String | Y | An activity defined by the source
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
FIPS | String | Y | FIPS code for location
FlowType | String | Y | `ELEMENTARY_FLOW`, `TECHNOSPHERE_FLOW`, or `WASTE_FLOW`. See <http://greendelta.github.io/olca-schema/html/FlowType.html>
Year | Int | Y | Year of data, e.g. `2010`
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md)
DataCollection | Numeric | Y | A score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md)
