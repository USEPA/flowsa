# flowsa

flowsa attributes resource use, waste, emissions, and loss to economic sectors. It produces standard tabular formats with
sector attribution data using pandas dataframes.

## Terms

_flows_: represent the physical movement of material, energy, entities or money as input or output to or between activities.
The terms __flows__ comes from life cycle assessment (ISO 14044).

_sectors_: generally these are economic sectors generating economic activity, but are extended
here to include households and institutional end users. Using the BEA definitions in input-output
tables, these can be either _industries_ or _commodities_.

_attribution_: The sectors through which activity uses, produces or receives the flows (input or output).

## Flow Classes

Class | Description | FlowBySector Reference Unit | [Flow Types](./formatspecs/FlowBySector.md) |
--- | --- | --- | --- |
Employment | Jobs | p | Modeled as ELEMENTARY_FLOWS produced by sectors |
Energy | Energy consumption, transfer as electricity or waste heat  | MJ | All types |
Land | Land area occupied | m2 per year | Modeled as ELEMENTARY_FLOWS consumed by sectors |
Money | Purchases | USDyear* | Modeled as TECHNOSPHERE_FLOWS with producing and consuming sectors | 
Water | Water use and release data, including wastewater | m3 | All types |
Other | Misc flows used for supporting data | _varies_ | All types |

*USD unit value varies by year and is reported like 'USD2012'

## FlowByActivity Datasets
 
Code | Dataset | Class | Geographic Scale | Description | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 |
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | 
USGS_Water_Use | [US Geological Survey Water Use in the US](https://www.usgs.gov/mission-areas/water-resources/science/water-use-united-states?qt-science_center_objects=0#qt-science_center_objects) | Water | County | Annual national level water use by various activities | x | NA | NA | NA| NA| x |NA |NA |NA |NA |
USDA_CoA_Cropland | [USDA Census of Agriculture](https://www.nass.usda.gov/Publications/AgCensus/2017/index.php#full_report) Horticultural Crop Area | Land | County | Crop area by farm size and irrigation status | NA | NA | X | NA| NA | NA | NA | x |NA |NA |
BLS_QCEW | [Bureau of Labor Statistics Quarterly Census of Employment and Wages](https://www.bls.gov/cew/) | Employment | County | Number of employees per industry |  |  |  |  | X | X |  |  |  |  |
Census_CBP_EMP | [Census Bureau County Business Patterns](https://www.census.gov/programs-surveys/cbp.html) | Employment | County | Number of employees per industry |  |  |  |  | X |  |  | NA | NA | NA |
Census_CBP_PAYANN | [Census Bureau County Business Patterns](https://www.census.gov/programs-surveys/cbp.html) | Money | County | Annual payroll per industry |  |  |  |  | X |  |  | NA | NA | NA |
Census_CBP_ESTAB | [Census Bureau County Business Patterns](https://www.census.gov/programs-surveys/cbp.html) | Other | County | Number of establishments per industry |  |  |  |  | x |  |  | NA | NA | NA |

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.
