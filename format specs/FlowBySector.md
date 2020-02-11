## Flow-By-Sector Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
Flowable | String | Y | name of flow in its native source. See 'Flowable' in fedelemflowlist [FlowList](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/blob/master/format%20specs/FlowList.md) 
Class | String | Y | Class of flow 
Sector | String | Y | A valid NAICS code
SectorSourceName | String | Y | By default, `NAICS_2012_Code`
ReliabilityScore | Numeric | Y | A 1-5 score of data reliability based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
DataCollection | Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
Technological Correlation |  Numeric | Y | A 1-5 score of data collection based on reporting values associated with the amount see [US EPA Data Quality System](https://cfpub.epa.gov/si/si_public_record_report.cfm?dirEntryId=321834) and [Cashman et al. 2017](http://dx.doi.org/10.1021/acs.est.6b02160)
Context | String | Y | Name of compartment to which release goes, e.g. "air", "water", "ground". Used for inventory sources characterizing releases to multiple compartments.
FIPS | String | Y | FIPS code for location 
Unit | String | Y | SI unit acronym. 'kg' for mass flows; 'MJ' for energy flows
FlowType | String | Y | 'ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW', or 'WASTE_FLOW'. See http://greendelta.github.io/olca-schema/html/FlowType.html
Year | Int | Y | Year of data, e.g. `2010`

