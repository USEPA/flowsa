# flowsa
`flowsa` is a data processing library that attributes resource use, waste, emissions, and loss to economic sectors. `flowsa` aggregates, combines, and allocates data from a variety of sources. The sources can be found in the [GitHub wiki](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-activity-datasets) under "Flow-By-Activity Datasets".

`flowsa` helps support [USEEIO](https://www.epa.gov/land-research/us-environmentally-extended-input-output-useeio-technical-content) as part of the [USEEIO modeling](https://www.epa.gov/land-research/us-environmentally-extended-input-output-useeio-models) framework. The USEEIO models estimate potential impacts of goods and services in the US economy. The [Flow-By-Sector datasets](https://github.com/USEPA/flowsa/wiki/Available-Data#flow-by-sector-datasets) created in FLOWSA are the environmental inputs to [`useeior`](https://github.com/USEPA/useeior).

## Usage
### Flow-By-Activity Datasets
Flow-By-Activity datasets are formatted tables from a variety of sources. They are largely unchanged from the original data source, with the exception of formatting.

`import flowsa` \
`flowsa.seeAvailableFlowByModels('FBA')` \
`flowsa.getFlowByActivity(datasource="USDA_CoA_Cropland", year=2017)`

### Flow-By-Sector Datasets
Flow-By-Sector datasets are tables of environmental and other data attributed to [sectors](https://www.census.gov/naics/).

`import flowsa` \
`flowsa.seeAvailableFlowByModels('FBS')` \
`flowsa.getFlowBySector('Water_national_2015_m1')`

## Installation
`pip install https://github.com/USEPA/flowsa/archive/refs/tags/v0.3.2.zip`

### Additional Information on Installation, Examples, Detailed Documentation
For more information on `flowsa` see the [wiki](https://github.com/USEPA/flowsa/wiki).

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis
and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer
has responsibility to protect the integrity , confidentiality, or availability of the information.  Any
reference to specific commercial products, processes, or services by service mark, trademark, manufacturer,
or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal
and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or
the United States Government.
