# Activity-to-Sector Mapping Format

Field | Type | Required? | Description
----- | ---- | --------  | -----------
ActivitySourceName | String | Y | `SourceName` in [FlowByActivity](FlowByActivity.md).
Activity | String | Y | `Activity` in [FlowByActivity](FlowByActivity.md).
SectorSourceName |  String | Y | `SectorSourceName` in [FlowBySector](FlowBySector.md).
Sector | String | Y | `Sector` in [FlowBySector](FlowBySector.md).
SectorType | String | N | "I" for industry or "C" for commodity
