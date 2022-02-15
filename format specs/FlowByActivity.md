# Flow-By-Activity Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Class | String | Y | Class of the flow. See [Flow Classes](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-classes).   
SourceName | String | Y | Name of data source
FlowName | String | Y | ID or name of flow in its native source
FlowAmount | Numeric | Y | The amount of a given flow in its native unit
Unit | String | Y | Unit of flow as provided by source
FlowType | String | Y | `ELEMENTARY_FLOW`, `TECHNOSPHERE_FLOW`, or `WASTE_FLOW`. See <http://greendelta.github.io/olca-schema/FlowType.html>
ActivityProducedBy | String | N* | An activity defined by the source producing a flow.
ActivityConsumedBy | String | N* | An activity defined by the source receiving/consuming a flow.
Compartment | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". 
Location | String | Y | A numeric representation of the activity location, at a national, state, or county level
LocationSystem | String | Y | Description and year of the Location code, generally FIPS or ISO, e.g. `FIPS_2015`
Year | Int | Y | Year of data, e.g. `2010`
MeasureofSpread | String | N | A measure of spread of a frequency distribution. Acceptable values are `RSD` for relative standard deviation (aka coefficient of variation) are `SD` for the normal (aka 'arithmatic') standard deviation, `GSD` for geometric standard deviation
Spread | Numeric | N | The value for the given measure of spread. 
DistributionType | String | N | The form of the frequency distribution, if given. Acceptable values are 'NORMAL', 'LOGNORMAL', 'TRIANGULAR', 'UNIFORM'.
Min | Numeric | N | The minimum FlowAmount, if provided for the data range. 
Max | Numeric | N | The maximum FlowAmount, if provided for the data range.
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md)
DataCollection | Numeric | Y | A score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md)
Description | String | Y | Original description of the flow

*At minimum, either ActivityProducedBy or ActivityConsumedBy must be present. If there is a transfer between activities, both must be present.