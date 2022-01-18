# USDA_ERS_MLU.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
USDA Economic Research Service (ERS) Major Land Uses (MLU)
https://www.ers.usda.gov/data-products/major-land-uses/
Last updated: Thursday, April 16, 2020
"""

import io
import pandas as pd
import numpy as np
from flowsa.common import get_all_state_FIPS_2, US_FIPS
from flowsa.settings import vLogDetailed
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.common import load_crosswalk
from flowsa.literature_values import \
    get_area_of_rural_land_occupied_by_houses_2013, \
    get_area_of_urban_land_occupied_by_houses_2013, \
    get_transportation_sectors_based_on_FHA_fees, \
    get_urban_land_use_for_airports, \
    get_urban_land_use_for_railroads, get_open_space_fraction_of_urban_area
from flowsa.validation import compare_df_units


def mlu_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as fp:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    return df


def mlu_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False
                   ).drop(columns=['SortOrder', 'Region']
                          ).rename(columns={'Region or State': 'State'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["State", "Year"],
                 var_name="FlowName",
                 value_name="FlowAmount")

    # load fips codes and merge
    fips = get_all_state_FIPS_2()
    fips['State'] = fips['State'].apply(lambda x: x.title())
    fips['FIPS_2'] = fips['FIPS_2'] + '000'
    dfm = df.merge(fips, how='left').rename(columns={'FIPS_2': 'Location'})
    dfm['Location'] = np.where(dfm['State'] == "U.S. total",
                               US_FIPS, dfm['Location'])

    # drop null values
    dfm2 = dfm[~dfm['Location'].isnull()].reset_index(drop=True)
    dfm3 = assign_fips_location_system(dfm2, year)
    # sub by year
    dfm3['Year'] = dfm3['Year'].astype(str)
    dfm3 = dfm3[dfm3['Year'] == year].reset_index(drop=True)

    dfm3["Class"] = "Land"
    dfm3["SourceName"] = source
    dfm3["ActivityProducedBy"] = None
    dfm3["ActivityConsumedBy"] = dfm3['FlowName']
    dfm3['FlowType'] = 'ELEMENTARY_FLOW'
    dfm3["Compartment"] = 'ground'
    dfm3["Unit"] = "Thousand Acres"
    dfm3['DataReliability'] = 5  # tmp
    dfm3['DataCollection'] = 5  # tmp

    return dfm3


def allocate_usda_ers_mlu_land_in_urban_areas(df, attr, fbs_list):
    """
    This function is used to allocate the USDA_ERS_MLU activity 'land in
    urban areas' to NAICS 2012 sectors. Allocation is dependent on
    assumptions defined in 'literature_values.py' as well as results from
    allocating 'EIA_CBECS_Land' and 'EIA_MECS_Land' to land based sectors.

    Methodology is based on the manuscript:
    Lin Zeng and Anu Ramaswami
    Impact of Locational Choices and Consumer Behaviors on Personal
    Land Footprints: An Exploration Across the Urban–Rural Continuum in the
    United States
    Environmental Science & Technology 2020 54 (6), 3091-3102
    DOI: 10.1021/acs.est.9b06024

    :param df: df, USDA ERA MLU Land
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fbs_list: list, FBS dfs for activities created prior
                     to the activity set that calls on this fxn
    :return: df, allocated USDS ERS MLU Land, FBS format
    """

    # define sector column to base calculations
    sector_col = 'SectorConsumedBy'

    vLogDetailed.info('Assuming total land use from MECS and CBECS included '
                      'in urban land area, so subtracting out calculated '
                      'MECS and CBECS land from MLU urban land area')
    # read in the cbecs and mecs df from df_list
    for df_i in fbs_list:
        if (df_i['MetaSources'] == 'EIA_CBECS_Land').all():
            cbecs = df_i
        elif (df_i['MetaSources'] == 'EIA_MECS_Land').all():
            mecs = df_i

    # load the federal highway administration fees dictionary
    fha_dict = get_transportation_sectors_based_on_FHA_fees()
    df_fha = pd.DataFrame.from_dict(
        fha_dict, orient='index').rename(
        columns={'NAICS_2012_Code': sector_col})

    # calculate total residential area from the American Housing Survey
    residential_land_area = get_area_of_urban_land_occupied_by_houses_2013()
    df_residential = df[df[sector_col] == 'F01000']
    df_residential = df_residential.assign(FlowAmount=residential_land_area)

    # make an assumption about the percent of urban area that is open space
    openspace_multiplier = get_open_space_fraction_of_urban_area()
    df_openspace = df[df[sector_col] == '712190']
    df_openspace = df_openspace.assign(
        FlowAmount=df_openspace['FlowAmount'] * openspace_multiplier)

    # sum all uses of urban area that are NOT transportation
    # first concat dfs for residential, openspace, commercial,
    # and manufacturing land use
    df_non_urban_transport_area = pd.concat(
        [df_residential, df_openspace, cbecs, mecs], sort=False,
        ignore_index=True)
    df_non_urban_transport_area = \
        df_non_urban_transport_area[['Location', 'Unit', 'FlowAmount']]
    non_urban_transport_area_sum = df_non_urban_transport_area.groupby(
            ['Location', 'Unit'], as_index=False).agg(
        {'FlowAmount': sum}).rename(columns={'FlowAmount': 'NonTransport'})
    # compare units
    compare_df_units(df, df_non_urban_transport_area)
    # calculate total urban transportation by subtracting
    # calculated areas from total urban land
    df_transport = df.merge(non_urban_transport_area_sum, how='left')
    df_transport = df_transport.assign(
        FlowAmount=df_transport['FlowAmount'] - df_transport['NonTransport'])
    df_transport.drop(columns=['NonTransport'], inplace=True)

    # make an assumption about the percent of urban transport
    # area used by airports
    airport_multiplier = get_urban_land_use_for_airports()
    df_airport = df_transport[df_transport[sector_col] == '488119']
    df_airport = df_airport.assign(
        FlowAmount=df_airport['FlowAmount'] * airport_multiplier)

    # make an assumption about the percent of urban transport
    # area used by railroads
    railroad_multiplier = get_urban_land_use_for_railroads()
    df_railroad = df_transport[df_transport[sector_col] == '482112']
    df_railroad = df_railroad.assign(
        FlowAmount=df_railroad['FlowAmount'] * railroad_multiplier)

    # further allocate the remaining urban transportation area using
    # Federal Highway Administration fees
    # first subtract area for airports and railroads
    air_rail_area = pd.concat([df_airport, df_railroad], sort=False)
    air_rail_area = air_rail_area[['Location', 'Unit', 'FlowAmount']]
    air_rail_area_sum = air_rail_area.groupby(
        ['Location', 'Unit'], as_index=False).agg(
        {'FlowAmount': sum}).rename(columns={'FlowAmount': 'AirRail'})

    df_highway = df_transport.merge(air_rail_area_sum, how='left')
    df_highway = df_highway.assign(
        FlowAmount=df_highway['FlowAmount'] - df_highway['AirRail'])
    df_highway.drop(columns=['AirRail'], inplace=True)

    # add fed highway administration fees
    df_highway2 = df_highway.merge(df_fha, how='left')
    df_highway2 = df_highway2[df_highway2['ShareOfFees'].notna()]
    df_highway2 = df_highway2.assign(
        FlowAmount=df_highway2['FlowAmount'] * df_highway2['ShareOfFees'])
    df_highway2.drop(columns=['ShareOfFees'], inplace=True)

    # concat all df subsets
    allocated_urban_areas_df = pd.concat(
        [df_residential, df_openspace, df_airport, df_railroad, df_highway2],
        ignore_index=True, sort=False).reset_index(drop=True)

    return allocated_urban_areas_df


def allocate_usda_ers_mlu_land_in_rural_transportation_areas(
        df, attr, fbs_list):
    """
    This function is used to allocate the USDA_ERS_MLU activity
    'land in urban areas' to NAICS 2012 sectors. Allocation
    is dependent on assumptions defined in 'literature_values.py'.

    Methodology is based on the manuscript:
    Lin Zeng and Anu Ramaswami
    Impact of Locational Choices and Consumer Behaviors on Personal
    Land Footprints: An Exploration Across the Urban–Rural Continuum in the
    United States
    Environmental Science & Technology 2020 54 (6), 3091-3102
    DOI: 10.1021/acs.est.9b06024

    :param df: df, USDA ERA MLU Land
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fbs_list: list, FBS dfs for activities created prior
                     to the activity set that calls on this fxn
    :return: df, allocated USDS ERS MLU Land, FBS format
    """

    # define sector column to base calculations
    sector_col = 'SectorConsumedBy'

    # load the federal highway administration fees dictionary
    fha_dict = get_transportation_sectors_based_on_FHA_fees()
    df_fha = pd.DataFrame.from_dict(fha_dict, orient='index').rename(
        columns={'NAICS_2012_Code': sector_col})

    # make an assumption about the percent of rural transport
    # area used by airports
    airport_multiplier = get_urban_land_use_for_airports()
    df_airport = df[df[sector_col] == '488119']
    df_airport = df_airport.assign(
        FlowAmount=df_airport['FlowAmount'] * airport_multiplier)

    # make an assumption about the percent of urban transport
    # area used by railroads
    railroad_multiplier = get_urban_land_use_for_railroads()
    df_railroad = df[df[sector_col] == '482112']
    df_railroad = df_railroad.assign(
        FlowAmount=df_railroad['FlowAmount'] * railroad_multiplier)

    # further allocate the remaining urban transportation area
    # using Federal Highway Administration fees
    # first subtract area for airports and railroads
    air_rail_area = pd.concat([df_airport, df_railroad], sort=False)
    air_rail_area = air_rail_area[['Location', 'Unit', 'FlowAmount']]
    air_rail_area_sum = air_rail_area.groupby(
        ['Location', 'Unit'], as_index=False).agg(
        {'FlowAmount': sum}).rename(columns={'FlowAmount': 'AirRail'})

    # compare units
    compare_df_units(df, air_rail_area)
    df_highway = df.merge(air_rail_area_sum, how='left')
    df_highway = df_highway.assign(
        FlowAmount=df_highway['FlowAmount'] - df_highway['AirRail'])
    df_highway.drop(columns=['AirRail'], inplace=True)

    # add fed highway administration fees
    df_highway2 = df_highway.merge(df_fha, how='left')
    df_highway2 = df_highway2[df_highway2['ShareOfFees'].notna()]
    df_highway2 = df_highway2.assign(
        FlowAmount=df_highway2['FlowAmount'] * df_highway2['ShareOfFees'])
    df_highway2.drop(columns=['ShareOfFees'], inplace=True)

    # concat airport, railroad, highway
    allocated_rural_trans = pd.concat(
        [df_airport, df_railroad, df_highway2], sort=False, ignore_index=True)

    return allocated_rural_trans


def allocate_usda_ers_mlu_other_land(df, attr, fbs_list):
    """
    From the USDA ERS MLU 2012 report:
    "Includes miscellaneous other uses, such as industrial and commercial
    sites in rural areas, cemeteries, golf courses, mining areas, quarry sites,
    marshes, swamps, sand dunes, bare rocks, deserts, tundra,
    rural residential, and other unclassified land. In this report,
    urban land is reported as a separate category."

    Mining data is calculated using a separate source = BLM PLS.
    Want to extract rural residential land area from total value of
    'Other Land'
    :param df: df, USDA ERA MLU Land
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fbs_list: list, FBS dfs for activities created prior to the activity
                     set that calls on this fxn
    :return: df, allocated USDS ERS MLU Land, FBS format
    """

    # land in rural residential lots
    rural_res = get_area_of_rural_land_occupied_by_houses_2013()

    # household codes
    household = load_crosswalk('household')
    household = household['Code'].drop_duplicates().tolist()

    # in df, where sector is a personal expenditure value, and
    # location = 00000, replace with rural res value
    vLogDetailed.info('The only category for MLU other land use is rural land '
                      'occupation. All other land area in this category is '
                      'unassigned to sectors, resulting in unaccounted land '
                      'area.')
    df['FlowAmount'] = np.where(df['SectorConsumedBy'].isin(household),
                                rural_res, df['FlowAmount'])

    return df
