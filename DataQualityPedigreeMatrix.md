# Data Quality Pedigree Mattix

The following table is an update of
'Table 3 Updated Data Quality Pedigree Matrix – Flow Indicators' from [USEPA 2016 'Guidance on Data Quality Assessment for Life Cycle Inventory Data'](https://cfpub.epa.gov/si/si_public_record_report.cfm?Lab=NRMRL&dirEntryId=321834)
for application to FLOWSA.

Indicator | 1 | 2 | 3 | 4 | 5 | Applies to |
---|---|---|---|---|---|---|
DataReliability | Verified data based on measurements | Verified data based on a calculation OR non-verified data based on measurements | Non-verified data based on a calculation | Documented estimate | Undocumented estimate | [FlowByActivity](./format specs/FlowByActivity.md), [FlowBySector](./format specs/FlowBySector.md)
TemporalCorrelation | Less than 3 years of difference | Less than 6 years of difference | Less than 10 years of difference | Less than 15 years of difference | Age of data unknown or more than 15 years | [FlowBySector](./format specs/FlowBySector.md)
GeographicalCorrelation | Data represent same FIPS code and level or higher resolution  | Data represent different FIPS code within the same level or 1 level lower resolution | Data represent different FIPS code and off two levels or 2 levels lower resolution |  Data represent different FIPS code within the same level or 3 levels lower resolution | Data geography just a proxy or unknown | [FlowBySector](./format specs/FlowBySector.md) |
TechnologicalCorrelation | Data represent same NAICS sector and level or higher resolution | Data have last 1 NAICSs sector difference  or 1 level lower resolution | Data have last 2 NAICS sector difference or 2 levels lower resolution | Data represent last 3 NAICS sectors off or 3 levels lower resolution  | Data represent last 4 NAICS sectors off or 4 levels lower resolution or data applied across all technologies | [FlowBySector](./format specs/FlowBySector.md) |
DataCollection | >= 80% of product represented or % of establishments of activities reporting | 60 <= x <= 80% of of product represented or % establishments or activities reporting | 40% >= x >= 60% of product represented or % of establishments or activities reporting | <= 40% of product represented or % of establishments or activities reporting | unknown percentage of establishments or activities reporting | [FlowByActivity](./format specs/FlowByActivity.md), [FlowBySector](./format specs/FlowBySector.md)

## Rules

1. Data can represent the original raw daw or the data used for sector allocation.  
2. Allocation of amount data with data representing another flow results in a 1 point quality deduction for TechnologicalCorrelation.
3. Lack of certainty over whether an activity represents a commodity or an industry results in a 1 point quality deduction for TechnologicalCorrelation.
4. For DataCollection, percentage of production value, mass or activity is preferred over number of establishments.

## Notes

1. DataReliability and TemporalCorrelation are unchanged from USEPA 2016.

## Data Quality Scoring Examples

### Fictional seafood industry example

Target Data

Data Type | Time | Geography | Sector | Sample |
---|---|---|---|---|
Mass of seafood input | 2020 | CA | Seafood Product Preparation and Packaging (industry) | NA |

Source Data

Data Type | Time | Geography | Sector | Sample|  
---|---|---|---|---|
Documented estimate of seafood input | ~1940 | Monterey,CA | Sardine canning | 1 establishment |

Score

DataReliability | TemporalCorrelation | GeographicalCorrelation | TechnologicalCorrelation | DataCollection |
---|---|---|---|---|
4 | 5 | 1 | 1 | 5 |

Explanation
Data were based on an estimate that was documented (4). The time is ~80 years before the target date (5). The geography represents
1 city/town which is a higher resolution than required (for California) (1). Data represent a more resolved industry that required (sardine canning) (1).
Data are only from 1 facility out of an unknown total CA industry, so it's assumed to represent a <40% (5).
