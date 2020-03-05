# flowsa

flowsa attributes resource use, waste, emissions, and loss to economic sectors. It produces standard tabular formats with
sector attribution data using pandas dataframes.

## Terms

_flows_: represent the physical movement of material or energy as input or output to or between activities.
In LCA terms these are more strictly elementary flows or waste flows, although these LCA uses will not limit the scope of FLOWSA models.

_sectors_: generally these are economic sectors generating economic activity, but are extended
here to include households and institutional end users. Using the BEA definitions in input-output
tables, these can be either _industries_ or _commodities_.

_attribution_: The sectors through which activity uses, produces or receives the flows (input or output).

## Flow Classes

Class | Description | [Flow Types](./formatspecs/FlowBySector.md) |
--- | --- | --- |
Employment | Jobs | Modeled as ELEMENTARY_FLOWS produced by sectors |
Energy | Energy consumption, transfer as electricity or waste heat  | All types |
Land | Land area occupied | Modeled as ELEMENTARY_FLOWS consumed by sectors |
Money | Purchases | Modeled as TECHNOSPHERE_FLOWS with producing and consuming sectors | 
Water | Water use and release data, including wastewater | All types |


## FlowByActivity Datasets
 
Code | Dataset | Class | Geographic Scale | Description | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 |
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | 
USGS_Water_Use | [US Geological Survey Water Use in the US](https://www.usgs.gov/mission-areas/water-resources/science/water-use-united-states?qt-science_center_objects=0#qt-science_center_objects) | Water | County | Annual national level water use by various activities | x | NA | NA | NA| NA| x |NA |NA |NA |NA |
USDA_CoA_Cropland | [USDA Census of Agriculture](https://www.nass.usda.gov/Publications/AgCensus/2017/index.php#full_report) Horticultural Crop Area | Land | County | Crop area by farm size and irrigation status | NA | NA | X | NA| NA | NA | NA | x |NA |NA |
BLS_QCEW | [Bureau of Labor Statistics Quarterly Census of Employment and Wages](https://www.bls.gov/cew/) | Employment | County | Number of employees | X | X | X | X | X | X | X | X | X | X |
Census_CBP | [Census Bureau County Business Patterns](https://www.census.gov/programs-surveys/cbp.html) | Employment | County | Number of employees |  |  | X | X | X | X | X | NA | NA | NA |


## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.
