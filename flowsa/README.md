# Datapull Descriptions
Descriptions of the type of data pulled from each data source and the information in the 
FlowByActivity parquet files.

## BLS_QCEW
US Bureau of Labor Statistics, Quarterly Census of Employment and Wages

## Census_CBP
US Census Bureau, County Business Patterns

## Census_PEP_Population
US Census Bureau, Population Estimates Program, Population

## EIA_CBECS_Land
US Energy Information Administration, Commercial Buildings Energy Consumption Survey, Land

## EIA_CBECS_Water
US Energy Information Administration, Commercial Buildings Energy Consumption Survey, Water

## [EIA_MECS_Energy](https://www.eia.gov/consumption/manufacturing/)
US Energy Information Administration, Manufacturing Energy Consumption Survey
- Energy (Tables 2.1, 2.2, 3.1, 3.2)
- National and regional (4 Census regions)
- Flows in energy units (MJ) and physical units (varies), represents duplicate data
- Fuel consumption Class: Energy
- Nonfuel consumption (feedstock) Class: Other

## [EPA_NEI](https://www.epa.gov/air-emissions-inventories/national-emissions-inventory-nei)
Environmental Protection Agency National Emissions Inventory
- Nonpoint, Nonroad, Onroad emissions
- County level 

## NOAA_FisheryLandings
National Oceanic and Atmospheric Administration, Fishery Landings

## [StatCan_IWS_MI]('https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3810003701')
Statistics Canada, Industrial Water Survey, Manufacturing Industries
- Table: 38-10-0037-01 (formerly CANSIM 153-0047)

## [USDA_CoA_Cropland]('https://www.nass.usda.gov/AgCensus/')
US Department of Agriculture, Census of Agriculture, Cropland
- National, state, county levels
- Total cropland and pastureland
- Harvested cropland and pastureland
- Harvested, irrigated cropland and pastureland

## [USDA_CoA_Livestock]('https://www.nass.usda.gov/AgCensus/')
US Department of Agriculture, Census of Agriculture, Livestock
- Tables 12, 15-17, 19, 27, 27-30, 32 (2017 report @ national level)
- National, state, county levels
- Livestock inventory for animal types

## [USDA_ERS_FIWS]('https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/data-files-us-and-state-level-farm-income-and-wealth-statistics/')
US Department of Agriculture, Economic Research Service, Farm Income and Wealth Statistics
- National, state level
- Cash Receipts by commodity for US crops and animals

## [USDA_ERS_MLU]('https://www.ers.usda.gov/data-products/major-land-uses/')
US Department of Agriculture, Economic Research Service, Major Land Use
- National level
- Major uses of public/private land for 15 land use categories in Thousand Acres

## [USDA_IWMS]('https://www.nass.usda.gov/Publications/AgCensus/2017/Online_Resources/Farm_and_Ranch_Irrigation_Survey/index.php')
US Department of Agriculture, Irrigation and Water Management Survey
- Table 36: Field Water Distribution for Selected Crops Harvested in the Open and Irrigated 
  Pastureland: 2018 and 2013 (2018 report)
- National, State level
- Water Application rates in Average acre-feet applied per acre by crop type

## [USGS_NWIS_WU]('https://waterdata.usgs.gov/nwis')
US Geological Survey, National Water Information System, Water Use 
- National, State, County level water withdrawals for the US, originally in million gallons per day
- Water withdrawals for ground/surface/total water, fresh/saline/total water
- Withdrawals for Aquaculture, Public Supply, Domestic Deliveries, Livestock, Irrigation (Crop and Golf), 
  Thermoelectric Power, Industrial, Mining

## [USGS_WU_Coef]('https://pubs.er.usgs.gov/publication/sir20095041')
US Geological Survey, Water Use Coefficients
- Source: Lovelace, John K., 2009, Method for estimating water withdrawals for livestock in the 
  United States, 2005: U.S. Geological Survey Scientific Investigations Report 2009–5041, 7 p.
- Table 1
- Livestock water use originally provided in gallons/animal/day for 9 animal types based on 2005 USGS NWIS WU
- Ground and surface water associated with livestock watering, feedlots, dairy operations, and other on-farm needs. 
  Water for drinking, cooling, sanitation, waste disposal, and other needs.

