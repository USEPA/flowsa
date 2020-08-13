Description of parameters in flowbysectormethods yamls

target_sector_level: specify desired sector aggregation, ranging from 2-6 digits

target_sector_source: specify NAICS version 2007, 2012, 2017. Recommend NAICS_2012_Code, as the majority of datasets use 
this version of NAICS

target_geoscale: national, state, or county

flowbyactivity_sources: Currently only set up for 'USGS_NWIS_WU'

class: a variable in 'Class' flowbyactivity

geoscale_to_use: national, state, or county

year: year of avialable dataset

activity_sets: Each declared activity set will result in a flowbysector dataset

names: specify an activity name found in USGS_NWIS_WU

allocation_source: The primary data source used used to allocate direct-water use for speciifed activity to sectors

allocation_method: currently written for 'direct' and 'proportional'

allocation_source_class: specific 'FlowClass' found in the allocation source flowbyactivity parquet

allocation_sector_aggregation: 'agg' (aggregated) or 'disagg' (disaggregated) depending on the allocation source. Some
of the allocation dataset crosswalks contain every level of relevant sectors (ex. NAICS for 2-6 digits), so the dataset 
should remain 'agg' because if the sector levels are 'disagg', there will be double counting. Other datasets only 
contain information for the highest relevant sector level, in which case, the allocation source should be 'disagg' to
include all relevant more specific sectors (ex. USGS_WU_Coef crosswalk)

allocation_source_year: specific to the allocation datasets, use the year relevant to the USGS_NWIS_WU dataframe

allocation_flow: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet

allocation_compartment: a list of relevant 'Compartment' values, as found in the source flowbyactivity parquet

allocation_from_scale: national, state, or county - dependent on allocation source, as not every level exits for sources

allocation_helper: 'yes' if second dataset is needed for allocation, 'no' if not

helper_source: secondary df for sector allocation

helper_method: currently written for 'multiplication'

helper_source_class: specific 'FlowClass' found in the allocation source flowbyactivity parquet

helper_sector_aggregation: 'agg' or 'disagg'

helper_source_year: specific to the allocation datasets, use the year relevant to the USGS_NWIS_WU dataframe

helper_flow: a list of relevant 'FlowName' values, as found in the source flowbyactivity parquet

helper_from_scale: national, state, or county - dependent on allocation source, as not every level exits for sources
