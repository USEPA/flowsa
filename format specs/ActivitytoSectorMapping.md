## Activity-to-Sector Mapping Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
ActivitySourceName | string | Y | `SourceName` in [FlowByActivity](FlowByActivity.md).
Activity | String | Y | `Activity` in [FlowByActivity](FlowByActivity.md). 
SectorSourceName | Y | `SectorSourceName` in [FlowBySector](FlowBySector.md). 
Sector | String | Y | `Sector` in [FlowBySector](FlowBySector.md).
Technological Correlation |  Numeric | Y | A 1-5 score of technology collection based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
