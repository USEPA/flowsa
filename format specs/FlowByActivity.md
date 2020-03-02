# Flow-By-Activity Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Class | String | Y | Class of the flow. See [Flow Classes](./README.md#FlowClasses).   
SourceName | String | Y | ID or name of flow in its native source
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow released to a given environment compartment or waste generated in a reference unit. Uses metric reference units. 'kg' is the reference unit for mass; 'MJ' is the unit for energy.
Unit | String | Y | SI unit acronym. `kg` for mass flows; `MJ` for energy flows
ActivityProducedBy | String | N* | An activity defined by the source producing a flow.
ActivityConsumedBy | String | N* | An activity defined by the source receiving/consuming a flow.
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used 
FIPS | String | Y | FIPS code for location
Year | Int | Y | Year of data, e.g. `2010`
MeasureofSpread | String | N | A measure of spread of a frequency distribution. Acceptable values are `RSD` for relative standard deviation (aka coefficient of variation) are `SD` for the normal (aka 'arithmatic') standard deviation, `GSD` for geometric standard deviation
Spread | Numeric | N | The value for the given measure of spread. 
DistributionType | String | N | The form of the frequency distribution, if given. Acceptable values are 'NORMAL', 'LOGNORMAL', 'TRIANGULAR', 'UNIFORM'.
Min | Numeric | N | The minimum FlowAmount, if provided for the data range. 
Max | Numeric | N | The maximum FlowAmount, if provided for the data range.
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md)
DataCollection | Numeric | Y | A score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md)
Description | String | Y | Original description of the flow

*At minimum, either ActivityProducedBy or ActivityConsumedBy must be present. If there is a transfer between activities, both must be present.