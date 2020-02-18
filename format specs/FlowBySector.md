# Flow-By-Sector Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Flowable | String | Y | name of flow in its native source. See 'Flowable' in fedelemflowlist [FlowList](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/blob/master/format%20specs/FlowList.md)
Class | String | Y | Class of flow
Sector | String | Y | A valid NAICS code
SectorSourceName | String | Y | By default, `NAICS_2012_Code`
Context | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
FIPS | String | Y | FIPS code for location
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows
FlowType | String | Y | `ELEMENTARY_FLOW`, `TECHNOSPHERE_FLOW`, or `WASTE_FLOW`. See <http://greendelta.github.io/olca-schema/html/FlowType.html>
Year | Int | Y | Year of data, e.g. `2010`
DataReliability | Numeric | Y | A score of data reliability based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md)
TemporalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md).
GeographicalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md).
TechnologicalCorrelation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md).
DataCollection | Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount. See [Data Quality Pedigree Matrix](../DataQualityPedigreeMatrix.md).

For FlowType 'ELEMENTARY_FLOW', flows by default will use the Federal LCA Commons Elementary Flow List from [fedelemflowlist](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List),
which specifies the fields `Flowable`,`Class`,`Context`, and `Unit` in common with this specification.
