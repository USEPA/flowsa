# flowsa

flowsa attributes resource use, waste, emissions, and loss to economic sectors. It produces standard tabular formats with
sector attribution data using pandas dataframes.

## Installation, Examples, Detailed Documentation
For installation instructions, example code, and further explanation of the code, see the [wiki](https://github.com/USEPA/flowsa/wiki).

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
Chemicals | Chemicals and groups of chemicals as defined in the [Federal Elementary Flow List](https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List) | kg | Modeled as ELEMENTARY_FLOWS produced by sectors |
Employment | Jobs | p | Modeled as ELEMENTARY_FLOWS produced by sectors |
Energy | Energy consumption, transfer as electricity or waste heat  | MJ | All types |
Geological | Mineral and metal use | kg | All types |
Land | Land area occupied | m2 | Modeled as ELEMENTARY_FLOWS consumed by sectors |
Money | Purchases | USDyear* | Modeled as TECHNOSPHERE_FLOWS with producing and consuming sectors | 
Water | Water use and release data, including wastewater | kg | All types |
Other | Misc flows used for supporting data | _varies_ | All types |

*USD unit value varies by year and is reported like 'USD2012'

## FlowByActivity Datasets
 
Source data are imported and formatted into FlowByActivity datasets. The source data are only modified to meet column 
formatting criteria, meaning units are not standardized across "Class" types.
 
Code | Dataset | Class | Geographic Scale | Description | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 |
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | 
BLS_QCEW | [Bureau of Labor Statistics Quarterly Census of Employment and Wages](https://www.bls.gov/cew/) | Employment, Money, Other | County | Number of employees per industry |  |  |  |  | X | X |  |  |  |  |
Census_CBP | [Census Bureau County Business Patterns](https://www.census.gov/programs-surveys/cbp.html) | Employment, Money, Other | County | Number of employees per industry, Annual payroll per industry, Number of establishments per industry |  |  |  |  | X |  |  |  |  |  |
Census_PEP_Population | [Census Bureau Population Estimates](https://www.census.gov/programs-surveys/popest.html) | Other | County | Population | X | X | X | X | X | X | X | X | X | X | 
EIA_CBECS_Water| [Energy Information Administration Commercial Buildings Energy Consumption Survey](https://www.eia.gov/consumption/commercial/reports/2012/water/) | Water | Country | Water consumption in large buildings |  |  | X |  |  |  |  |  |  |  | 
EIA_MECS_Energy| [Energy Information Administration Manufacturing Energy Consumption Survey](https://www.eia.gov/consumption/manufacturing/) | Energy, Other | Region | Fuel and nonfuel consumption of energy flows by manufacturing industries | X |  |  |  | X |  |  |  |  |  | 
EPA_NEI_Nonpoint |[Environmental Protection Agency National Emissions Inventory Nonpoint sources](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei) | Chemicals | County | Air emissions of criteria pollutants, criteria precursors, and hazardous air pollutants |  |  |  |  |  |  |  | X |  |  | 
EPA_NEI_Nonroad |[Environmental Protection Agency National Emissions Inventory Nonroad sources](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei) | Chemicals | County | Air emissions of criteria pollutants, criteria precursors, and hazardous air pollutants |  |  |  |  |  |  |  | X |  |  |
EPA_NEI_Onroad |[Environmental Protection Agency National Emissions Inventory Onroad sources](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei) | Chemicals | County |  Air emissions of criteria pollutants, criteria precursors, and hazardous air pollutants |  |  |  |  |  |  |  | X |  |  |
NOAA_FisheryLandings | [National Oceanic and Atmospheric Administration Fisheries](https://foss.nmfs.noaa.gov/apexfoss/f?p=215:200) | Money | State | Fishery landings | X | X | X | X | X | X | X | X | X | X | 
StatCan_IWS_MI | [Statistics Canada Industrial Water Survey](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3810003701) | Water | Country | Water use by NAICS |  | X |  | X |  | X |  |  |  |  | 
USDA_CoA_Cropland | [USDA Census of Agriculture](https://www.nass.usda.gov/Publications/AgCensus/2017/index.php#full_report) | Land, Other | County | Crop area by farm size and irrigation status | | | X | | | |  | X | | |
USDA_CoA_Livestock | [USDA Census of Agriculture](https://www.nass.usda.gov/Publications/AgCensus/2017/index.php#full_report) | Other | County | Livestock count by farm size | | | X | | | |  | X | | | 
USDA_IWMS | [USDA Irrigation and Water Management Survey](https://www.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Farm_and_Ranch_Irrigation/) | Water | State | Water application rate by state and crop |  |  |  | X |  |  |  |  | X |  | 
USGS_NWIS_WU | [US Geological Survey Water Use in the US](https://www.usgs.gov/mission-areas/water-resources/science/water-use-united-states?qt-science_center_objects=0#qt-science_center_objects) | Water | County | Annual national level water use by various activities | X |  |  | | | X | | | | |

### FlowByActivity Naming Convention
Source dataset names are consistent across (1) the FlowByActivity dataset 'SourceName' columns, (2) the parquet file names,
(3) the Crosswalk file names, and (4) the Source Catalog information. Source names are comprised of two or three components.
The first part of the name is the agency that published the data. The second component is the name or acronym
of the published dataset. The third piece of the naming schema, if it exists, is the topic of data parsed from the 
original dataset. Of the four FlowByActivity datasets imported from the U.S. Department of Agriculture (USDA), three are
data pulled from the same dataset, the Census of Agriculture (CoA). To make data easier to find, the CoA data is separated 
by topic (Cropland, Livestock, Product Market Value). As the FlowByActivity datasets are grouped by topic, some of the 
parquets contain multiple class types, meaning the Class type should be specified when calling on the data. The
USDA_CoA_Cropland dataframe includes acreage information for crops (Class = Land) and the number of farms that grow a
particular crop (Class = Other). 

## FlowBySector Datasets

Environmental data attributed to North American Industrial Classification (NAICS) Codes, formatted into standard 
FlowBySector datasets. 
 
Description | Code | Years | Number of methods of sector allocation |
--- | --- | --- | --- |
Criteria and hazardous air emissions | CAP_HAP_national |  2017 | 1 |
Employment | Employment_national | 2017 | 1 |
Land use | Land_national |  2012 | 1 |
Water withdrawal | Water_national |  2010, 2015 | 2 |
Water withdrawal | Water_state |  2015 | 1 |
Point source releases to water | TRI_DMR_national | 2017 | 1 |
Commercial RCRA-defined hazardous waste | CRHW_national | 2017 | 1 |
Point source industrial releases to ground | GRDREL_national | 2017 | 1 | 

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.
