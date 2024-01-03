<!-- badges: start -->
[![FLOWSA Paper](http://img.shields.io/badge/FLOWSA%20Paper-10.3390/app12115742-blue.svg)](https://doi.org/10.3390/app12115742)
[![DOI](https://zenodo.org/badge/225456627.svg)](https://zenodo.org/badge/latestdoi/225456627)
<!-- badges: end -->

# flowsa
`flowsa` is a data processing library attributing the flows of resources 
(environmental, monetary, and human), wastes, emissions, and losses to sectors, typically 
[NAICS codes](https://www.census.gov/naics/). `flowsa` aggregates, combines,
and allocates data from a variety of sources. The sources can be found in the 
[GitHub wiki](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-activity-datasets) 
under "Flow-By-Activity Datasets".

`flowsa` helps support 
[USEEIO](https://www.epa.gov/land-research/us-environmentally-extended-input-output-useeio-technical-content) 
as part of the [USEEIO modeling](https://www.epa.gov/land-research/us-environmentally-extended-input-output-useeio-models) 
framework. The USEEIO models estimate potential impacts of goods and 
services in the US economy. The 
[Flow-By-Sector datasets](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-sector-datasets) 
created in FLOWSA are the environmental inputs to 
[`useeior`](https://github.com/USEPA/useeior).

## Usage
### Flow-By-Activity (FBA) Datasets
Flow-By-Activity datasets are formatted tables from a variety of sources. 
They are largely unchanged from the original data source, except for 
formatting. A list of available FBA datasets can be found in 
the [Wiki](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-activity-datasets).

`import flowsa` \
Return list of all available FBA datasets, including years 
`flowsa.seeAvailableFlowByModels('FBA')` \
Generate and return pandas dataframe for 2014 Energy Information 
Administration (EIA) Manufacturing Energy Consumption Survey (MECS) land use \
`fba = flowsa.getFlowByActivity(datasource="EIA_MECS_Land", year=2014)`

### Flow-By-Sector (FBS) Datasets
Flow-By-Sector datasets are tables of environmental and other data 
attributed to [sectors](https://www.census.gov/naics/). A list of available 
FBS datasets can be found in the 
[Wiki](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-sector-datasets).

`import flowsa` \
Return list of all available FBS datasets
`flowsa.seeAvailableFlowByModels('FBS')` \
Generate and return pandas dataframe for national water withdrawals 
attributed to 6-digit sectors. Download all required FBA datasets from 
Data Commons. \
`fbs = flowsa.getFlowBySector('Water_national_2015_m1', 
download_FBAs_if_missing=True)`

### Examples
Additional example code can be found in the [examples](https://github.com/USEPA/flowsa/tree/master/examples) folder.

## Installation
`pip install git+https://github.com/USEPA/flowsa.git@vX.X.X#egg=flowsa`

where vX.X.X can be replaced with the version you wish to install under 
[Releases](https://github.com/USEPA/flowsa/releases).

### Additional Information on Installation, Examples, Detailed Documentation
For more information on `flowsa` see the [wiki](https://github.com/USEPA/flowsa/wiki).

### Accessing datsets output by FLOWSA
FBA and FBS datasets can be accessed on 
[EPA's Data Commons](https://dmap-data-commons-ord.s3.amazonaws.com/index.html?prefix=flowsa/) without running the Python code. 

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code 
is provided on an "as is" basis and the user assumes responsibility for its 
use. EPA has relinquished control of the information and no longer has 
responsibility to protect the integrity, confidentiality, or availability 
of the information. Any reference to specific commercial products, 
processes, or services by service mark, trademark, manufacturer, or 
otherwise, does not constitute or imply their endorsement, recommendation 
or favoring by EPA. The EPA seal and logo shall not be used in any manner 
to imply endorsement of any commercial product or activity by EPA or
the United States Government.
