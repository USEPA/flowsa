# Flow-By-Sector Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Flowable | String | Y | Name of the flow. See 'Flowable' in fedelemflowlist [FlowList](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/blob/master/format%20specs/FlowList.md)
Class | String | Y | Class of flow
FlowAmount | Numeric | Y | The amount of a flow. Uses metric [reference units](./README.md#). 
SectorProducedBy | String | N* | A valid code from the SectorSourceName system (e.g. '31' for 'NAICS_2012_Code') 
SectorConsumedBy | String | N* | A valid code from the SectorSourceName system
SectorSourceName | String | Y | By default, `NAICS_2012_Code`. Must be the same for SectorProducedBy and SectorConsumedBy.
Context | String | Y | Full context for the flow, e.g. "air", "water", "ground". 
Location | String | Y | A numeric representation of the activity location, at a national, state, or county level
LocationSystem | String | Y | Description and year of the Location code, generally FIPS or ISO, e.g. `FIPS_2015`
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows.
FlowType | String | Y | `ELEMENTARY_FLOW`, `TECHNOSPHERE_FLOW`, or `WASTE_FLOW`. See <http://greendelta.github.io/olca-schema/FlowType.html>
Year | Int | Y | Year of data, e.g. `2010`
ProducedBySectorType | String | N | Commodity or Industry
ConsumedBySectorType | String | N | Commodity or Industry
MeasureofSpread | String | N | A measure of spread of a frequency distribution. Acceptable values are `RSD` for relative standard deviation (aka coefficient of variation) are `SD` for the normal (aka 'arithmatic') standard deviation, `GSD` for geometric standard deviation
Spread | Numeric | N | The value for the given measure of spread. 
DistributionType | String | N | The form of the frequency distribution, if given. Acceptable values are 'NORMAL', 'LOGNORMAL', 'TRIANGULAR', 'UNIFORM'.
Min | Numeric | N | The minimum FlowAmount, if provided for the data range. 
Max | Numeric | N | The maximum FlowAmount, if provided for the data range.
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md)
TemporalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md).
GeographicalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md).
TechnologicalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md).
DataCollection | Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../docs/DataQualityPedigreeMatrix.md).
MetaSources | String | Y | The major data source(s) the value is based on, usually a FlowByActivity set.
AttributionSources | String | Y | The primary attribution data source value is based on, usually a FlowByActivity set.
FlowUUID| String| Y | UUID from Federal Commons Flow List. 

* At minimum, either SectorProducedBy or SectorConsumedBy must be present. 
If there is a transfer between sectors, both must be present.

For FlowType 'ELEMENTARY_FLOW', flows by default will use the Federal LCA Commons Elementary Flow List from [fedelemflowlist](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List),
which specifies the fields `Flowable`,`Class`,`Context`, and `Unit` in common with this specification.

## Variants

### Flow-By-Sector Collapsed Format

In this variant, the fields `SectorProducedBy` and `SectorConsumedBy` are replaced by a `Sector` field. All other fields are identical.

| Field | Type | Required? | Description |
| --- | --- | ---  | --- |
| Sector | String | Y | A valid code from the SectorSourceName system |


