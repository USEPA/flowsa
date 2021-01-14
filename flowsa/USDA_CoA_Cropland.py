# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
import numpy as np
import pandas as pd
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system, collapse_activity_fields


def CoA_Cropland_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from flowbyactivity.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        for y in config['sector_levels']:
            # at national level, remove the text string calling for state acronyms
            if x == 'NATIONAL':
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__secLevel__", y)
                url = url.replace("&state_alpha=__stateAlpha__", "")
                if y == "ECONOMICS":
                    url = url.replace(
                        "AREA HARVESTED&statisticcat_desc=AREA IN PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc=AREA BEARING %26 NON-BEARING",
                        "AREA&statisticcat_desc=AREA OPERATED")
                else:
                    url = url.replace("&commodity_desc=AG LAND&commodity_desc=FARM OPERATIONS", "")
                url = url.replace(" ", "%20")
                urls.append(url)
            else:
                # substitute in state acronyms for state and county url calls
                for z in state_abbrevs:
                    url = build_url
                    url = url.replace("__aggLevel__", x)
                    url = url.replace("__secLevel__", y)
                    url = url.replace("__stateAlpha__", z)
                    if y == "ECONOMICS":
                        url = url.replace(
                            "AREA HARVESTED&statisticcat_desc=AREA IN PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc=AREA BEARING %26 NON-BEARING",
                            "AREA&statisticcat_desc=AREA OPERATED")
                    else:
                        url = url.replace("&commodity_desc=AG LAND&commodity_desc=FARM OPERATIONS", "")
                    url = url.replace(" ", "%20")
                    urls.append(url)
    return urls


def coa_cropland_call(url, coa_response, args):
    cropland_json = json.loads(coa_response.text)
    df_cropland = pd.DataFrame(data=cropland_json["data"])
    return df_cropland


def coa_cropland_parse(dataframe_list, args):
    """Modify the imported data so it meets the flowbyactivity criteria and only includes data on harvested acreage
    (irrigated and total). """

    df = pd.concat(dataframe_list, sort=False)
    # specify desired data based on domain_desc
    df = df[~df['domain_desc'].isin(['ECONOMIC CLASS', 'FARM SALES', 'IRRIGATION STATUS', 'CONCENTRATION',
                                     'ORGANIC STATUS', 'NAICS CLASSIFICATION', 'PRODUCERS'])]
    df = df[df['statisticcat_desc'].isin(['AREA HARVESTED', 'AREA IN PRODUCTION', 'AREA BEARING & NON-BEARING',
                                          'AREA', 'AREA OPERATED'])]
    # drop rows that subset data into farm sizes (ex. 'area harvested: (1,000 to 1,999 acres)
    df = df[~df['domaincat_desc'].str.contains(' ACRES')].reset_index(drop=True)
    # drop Descriptions that contain certain phrases, as these data are included in other categories
    df = df[~df['short_desc'].str.contains('FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP')]
    # drop Descriptions that contain certain phrases - only occur in AG LAND data
    df = df[~df['short_desc'].str.contains('INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE')].reset_index(drop=True)
    # Many crops are listed as their own commodities as well as grouped within a broader category (for example, orange
    # trees are also part of orchards). As this dta is not needed, takes up space, and can lead to double counting if
    # included, want to drop these unused columns
    # subset dataframe into the 5 crop types and land in farms and drop rows
    # crop totals: drop all data
    # field crops: don't want certain commodities and don't want detailed types of wheat, cotton, or sunflower
    df_fc = df[df['group_desc'] == 'FIELD CROPS']
    df_fc = df_fc[~df_fc['commodity_desc'].isin(['GRASSES', 'GRASSES & LEGUMES, OTHER', 'LEGUMES', 'HAY', 'HAYLAGE'])]
    df_fc = df_fc[~df_fc['class_desc'].str.contains('SPRING|WINTER|TRADITIONAL|OIL|PIMA|UPLAND', regex=True)]
    # fruit and tree nuts: only want a few commodities
    df_ftn = df[df['group_desc'] == 'FRUIT & TREE NUTS']
    df_ftn = df_ftn[df_ftn['commodity_desc'].isin(['BERRY TOTALS', 'ORCHARDS'])]
    df_ftn = df_ftn[df_ftn['class_desc'].isin(['ALL CLASSES'])]
    # horticulture: only want a few commodities
    df_h = df[df['group_desc'] == 'HORTICULTURE']
    df_h = df_h[df_h['commodity_desc'].isin(['CUT CHRISTMAS TREES', 'SHORT TERM WOODY CROPS'])]
    # vegetables: only want a few commodities
    df_v = df[df['group_desc'] == 'VEGETABLES']
    df_v = df_v[df_v['commodity_desc'].isin(['VEGETABLE TOTALS'])]
    # only want ag land and farm operations in farms & land & assets
    df_fla = df[df['group_desc'] == 'FARMS & LAND & ASSETS']
    df_fla = df_fla[df_fla['short_desc'].str.contains("AG LAND|FARM OPERATIONS")]
    # drop the irrigated acreage in farms (want the irrigated harvested acres)
    df_fla = df_fla[~((df_fla['domaincat_desc'] == 'AREA CROPLAND, HARVESTED: (ANY)') &
                    (df_fla['domain_desc'] == 'AREA CROPLAND, HARVESTED') &
                    (df_fla['short_desc'] == 'AG LAND, IRRIGATED - ACRES'))]
    # concat data frames
    df = pd.concat([df_fc, df_ftn, df_h, df_v, df_fla], sort=False).reset_index(drop=True)
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha', 'sector_desc',
                          'country_code', 'begin_code', 'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc', 'congr_district_code', 'asd_code',
                          'week_ending', 'freq_desc', 'load_time', 'zip_5', 'watershed_desc', 'region_desc',
                          'state_ansi', 'state_name', 'country_name', 'county_ansi', 'end_code', 'group_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'  # add county fips when missing
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS  # modify national level fips

    # address non-NAICS classification data
    # use info from other columns to determine flow name
    df.loc[:, 'FlowName'] = df['statisticcat_desc'] + ', ' + df['prodn_practice_desc']
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", ALL PRODUCTION PRACTICES", "", regex=True)
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", IN THE OPEN", "", regex=True)
    # combine column information to create activity information, and create two new columns for activities
    df['Activity'] = df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + df['util_practice_desc']  # drop this column later
    df['Activity'] = df['Activity'].str.replace(", ALL CLASSES", "", regex=True)  # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(", ALL UTILIZATION PRACTICES", "", regex=True)  # not interested in all data from class_desc
    df['ActivityProducedBy'] = np.where(df["unit_desc"] == 'OPERATIONS', df["Activity"], None)
    df['ActivityConsumedBy'] = np.where(df["unit_desc"] == 'ACRES', df["Activity"], None)

    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount", "unit_desc": "Unit",
                            "year": "Year", "CV (%)": "Spread",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['Activity', 'class_desc', 'commodity_desc', 'domain_desc', 'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc', 'domaincat_desc', 'util_practice_desc'])
    # modify contents of units column
    df.loc[df['Unit'] == 'OPERATIONS', 'Unit'] = 'p'
    # modify contents of flowamount column, "D" is supressed data, "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = withdrawn_keyword
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # USDA CoA 2017 states that (H) means CV >= 99.95, therefore replacing with 99.95 so can convert column to int
    # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None  # for instances where data is missing
    df.loc[df['Spread'] == "(D)", 'Spread'] = withdrawn_keyword
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # Add hardcoded data
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland"
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 2

    return df


def coa_irrigated_cropland_fba_cleanup(fba, **kwargs):
    """
    When using irrigated cropland, aggregate sectors to cropland and total ag land. Doing this because published values
    for irrigated harvested cropland do not include the water use for vegetables, woody crops, berries.
    :param fba:
    :return:
    """

    fba = fba[~fba['ActivityConsumedBy'].isin(['AG LAND', 'AG LAND, CROPLAND, HARVESTED'])]

    return fba


def disaggregate_coa_cropland_to_6_digit_naics(fba_w_sector, attr, method):
    """
    Disaggregate usda coa cropland to naics 6
    :param fba_w_sector:
    :param attr:
    :param method:
    :return:
    """

    # define the activity and sector columns to base modifications on
    # these definitions will vary dependent on class type
    activity_col = 'ActivityConsumedBy'
    sector_col = 'SectorConsumedBy'

    # drop rows without assigned sectors
    fba_w_sector = fba_w_sector[~fba_w_sector[sector_col].isna()]

    # modify the flowamounts related to the 6 naics 'orchards' are mapped to
    fba_w_sector = modify_orchard_flowamounts(fba_w_sector, activity_column=activity_col)

    # use ratios of usda 'land in farms' to determine animal use of pasturelands at 6 digit naics
    fba_w_sector = disaggregate_pastureland(fba_w_sector, attr, method, years_list=[attr['allocation_source_year']],
                                            sector_column=sector_col)

    # use ratios of usda 'harvested cropland' to determine missing 6 digit naics
    fba_w_sector = disaggregate_cropland(fba_w_sector, attr, method, years_list=[attr['allocation_source_year']],
                                         sector_column=sector_col)

    return fba_w_sector


def modify_orchard_flowamounts(fba, activity_column):
    """
    In the CoA cropland crosswalk, the activity 'orchards' is mapped to six 6-digit naics. Therefore, after mapping,
    arbitrarily divide the orchard flow amount by 6.
    :param fba: A FlowByActiivty df mapped to sectors
    :param activity_column: The activity column to base FlowAmount modifications
           on (ActivityProducedBy or ActivityConsumedBy)
    :return:
    """

    fba.loc[fba[activity_column] == 'ORCHARDS', 'FlowAmount'] = fba['FlowAmount'] / 6

    return fba


def disaggregate_pastureland(fba_w_sector, attr, method, years_list, sector_column):
    """
    The USDA CoA Cropland irrigated pastureland data only links to the 3 digit NAICS '112'. This function uses state
    level CoA 'Land in Farms' to allocate the county level acreage data to 6 digit NAICS.
    :param fba_w_sector: The CoA Cropland dataframe after linked to sectors
    :param attr:
    :param years_list:
    :param sector_column: The sector column on which to make df modifications (SectorProducedBy or SectorConsumedBy)
    :return: The CoA cropland dataframe with disaggregated pastureland data
    """

    import flowsa
    from flowsa.flowbyfunctions import allocate_by_sector, clean_df, flow_by_activity_fields, \
        fba_fill_na_dict, replace_strings_with_NoneType, replace_NoneType_with_empty_cells, \
        fba_mapped_default_grouping_fields, harmonize_units
    from flowsa.mapping import add_sectors_to_flowbyactivity

    # tmp drop NoneTypes
    fba_w_sector = replace_NoneType_with_empty_cells(fba_w_sector)

    # subset the coa data so only pastureland
    p = fba_w_sector.loc[fba_w_sector[sector_column].apply(lambda x: x[0:3]) == '112'].reset_index(drop=True)
    if len(p) != 0:
        # add temp loc column for state fips
        p = p.assign(Location_tmp=p['Location'].apply(lambda x: x[0:2]))
        df_sourcename = pd.unique(p['SourceName'])[0]

        # load usda coa cropland naics
        df_class = ['Land']
        df_years = years_list
        df_allocation = 'USDA_CoA_Cropland_NAICS'
        df_f = flowsa.getFlowByActivity(flowclass=df_class, years=df_years, datasource=df_allocation)
        df_f = clean_df(df_f, flow_by_activity_fields, fba_fill_na_dict)
        df_f = harmonize_units(df_f)
        # subset to land in farms data
        df_f = df_f[df_f['FlowName'] == 'FARM OPERATIONS']
        # subset to rows related to pastureland
        df_f = df_f.loc[df_f['ActivityConsumedBy'].apply(lambda x: x[0:3]) == '112']
        # drop rows with "&'
        df_f = df_f[~df_f['ActivityConsumedBy'].str.contains('&')]
        # create sector columns
        df_f = add_sectors_to_flowbyactivity(df_f, sectorsourcename=method['target_sector_source'])
        # create proportional ratios
        group_cols = fba_mapped_default_grouping_fields
        group_cols = [e for e in group_cols if
                      e not in ('ActivityProducedBy', 'ActivityConsumedBy')]
        df_f = allocate_by_sector(df_f, df_sourcename, df_allocation, 'proportional', group_cols)
        # tmp drop NoneTypes
        df_f = replace_NoneType_with_empty_cells(df_f)
        # drop naics = '11
        df_f = df_f[df_f[sector_column] != '11']
        # drop 000 in location
        df_f = df_f.assign(Location=df_f['Location'].apply(lambda x: x[0:2]))

        # merge the coa pastureland data with land in farm data
        df = p.merge(df_f[[sector_column, 'Location', 'FlowAmountRatio']], how='left',
                     left_on="Location_tmp", right_on="Location")
        # multiply the flowamount by the flowratio
        df.loc[:, 'FlowAmount'] = df['FlowAmount'] * df['FlowAmountRatio']
        # drop columns and rename
        df = df.drop(columns=['Location_tmp', sector_column + '_x', 'Location_y', 'FlowAmountRatio'])
        df = df.rename(columns={sector_column + '_y': sector_column,
                                "Location_x": 'Location'})

        # drop rows where sector = 112 and then concat with original fba_w_sector
        fba_w_sector = fba_w_sector[fba_w_sector[sector_column].apply(lambda x: x[0:3]) != '112'].reset_index(drop=True)
        fba_w_sector = pd.concat([fba_w_sector, df], sort=True).reset_index(drop=True)

        # fill empty cells with NoneType
        fba_w_sector = replace_strings_with_NoneType(fba_w_sector)

    return fba_w_sector


def disaggregate_cropland(fba_w_sector, attr, method, years_list, sector_column):
    """
    In the event there are 4 (or 5) digit naics for cropland at the county level, use state level harvested cropland to
    create ratios
    :param fba_w_sector:
    :param attr:
    :param years_list:
    :param sector_column: The sector column on which to make df modifications (SectorProducedBy or SectorConsumedBy)
    :param attr:
    :return:
    """

    import flowsa
    from flowsa.flowbyfunctions import sector_aggregation,\
        fbs_default_grouping_fields, clean_df, fba_fill_na_dict, fbs_fill_na_dict, add_missing_flow_by_fields,\
        sector_disaggregation, sector_ratios, replace_strings_with_NoneType, replace_NoneType_with_empty_cells,\
        harmonize_units
    from flowsa.mapping import add_sectors_to_flowbyactivity

    # tmp drop NoneTypes
    fba_w_sector = replace_NoneType_with_empty_cells(fba_w_sector)

    # drop pastureland data
    crop = fba_w_sector.loc[fba_w_sector[sector_column].apply(lambda x: x[0:3]) != '112'].reset_index(drop=True)
    # drop sectors < 4 digits
    crop = crop[crop[sector_column].apply(lambda x: len(x) > 3)].reset_index(drop=True)
    # create tmp location
    crop = crop.assign(Location_tmp=crop['Location'].apply(lambda x: x[0:2]))\

    # load the relevant state level harvested cropland by naics
    naics_load = flowsa.getFlowByActivity(flowclass=['Land'],
                                          years=years_list,
                                          datasource="USDA_CoA_Cropland_NAICS").reset_index(drop=True)
    # clean df
    naics = clean_df(naics_load, flow_by_activity_fields, fba_fill_na_dict)
    naics = harmonize_units(naics)
    # subset the harvested cropland by naics
    naics = naics[naics['FlowName'] == 'AG LAND, CROPLAND, HARVESTED'].reset_index(drop=True)
    # drop the activities that include '&'
    naics = naics[~naics['ActivityConsumedBy'].str.contains('&')].reset_index(drop=True)
    # add sectors
    naics = add_sectors_to_flowbyactivity(naics, sectorsourcename=method['target_sector_source'])
    # add missing fbs fields
    naics = clean_df(naics, flow_by_sector_fields, fbs_fill_na_dict)
    # drop cols and rename
    # naics = naics.drop(columns=["SectorProducedBy"])
    # naics = naics.rename(columns={"SectorConsumedBy": sector_column})

    # aggregate sectors to create any missing naics levels
    group_cols = fbs_default_grouping_fields
    # group_cols = [e for e in group_cols if e not in ('SectorProducedBy', 'SectorConsumedBy')]
    # group_cols.append(sector_column)
    naics2 = sector_aggregation(naics, group_cols)
    # add missing naics5/6 when only one naics5/6 associated with a naics4
    naics3 = sector_disaggregation(naics2, group_cols)
    # drop rows where FlowAmount 0
    # naics3 = naics3[~((naics3['SectorProducedBy'] == '') & (naics3['SectorConsumedBy'] == ''))]
    naics3 = naics3.loc[naics3['FlowAmount'] != 0]
    # create ratios
    naics4 = sector_ratios(naics3, sector_column)
    # create temporary sector column to match the two dfs on
    naics4 = naics4.assign(Location_tmp=naics4['Location'].apply(lambda x: x[0:2]))
    # tmp drop Nonetypes
    naics4 = replace_NoneType_with_empty_cells(naics4)

    # for loop through naics lengths to determine naics 4 and 5 digits to disaggregate
    for i in range(4, 6):
        # subset df to sectors with length = i and length = i + 1
        crop_subset = crop.loc[crop[sector_column].apply(lambda x: i+1 >= len(x) >= i)]
        crop_subset = crop_subset.assign(Sector_tmp=crop_subset[sector_column].apply(lambda x: x[0:i]))
        # if duplicates drop all rows
        df = crop_subset.drop_duplicates(subset=['Location', 'Sector_tmp'], keep=False).reset_index(drop=True)
        # drop sector temp column
        df = df.drop(columns=["Sector_tmp"])
        # subset df to keep the sectors of length i
        df_subset = df.loc[df[sector_column].apply(lambda x: len(x) == i)]
        # subset the naics df where naics length is i + 1
        naics_subset = naics4.loc[naics4[sector_column].apply(lambda x: len(x) == i+1)].reset_index(drop=True)
        naics_subset = naics_subset.assign(Sector_tmp=naics_subset[sector_column].apply(lambda x: x[0:i]))
        # merge the two df based on locations
        df_subset = pd.merge(df_subset, naics_subset[[sector_column, 'FlowAmountRatio', 'Sector_tmp', 'Location_tmp']],
                      how='left', left_on=[sector_column, 'Location_tmp'], right_on=['Sector_tmp', 'Location_tmp'])
        # create flow amounts for the new NAICS based on the flow ratio
        df_subset.loc[:, 'FlowAmount'] = df_subset['FlowAmount'] * df_subset['FlowAmountRatio']
        # drop rows of 0 and na
        df_subset = df_subset[df_subset['FlowAmount'] != 0]
        df_subset = df_subset[~df_subset['FlowAmount'].isna()].reset_index(drop=True)
        # drop columns
        df_subset = df_subset.drop(columns=[sector_column + '_x', 'FlowAmountRatio', 'Sector_tmp'])
        # rename columns
        df_subset = df_subset.rename(columns={sector_column + '_y': sector_column})
        # tmp drop Nonetypes
        df_subset = replace_NoneType_with_empty_cells(df_subset)
        # add new rows of data to crop df
        crop = pd.concat([crop, df_subset], sort=True).reset_index(drop=True)

    # clean up df
    crop = crop.drop(columns=['Location_tmp'])

    # pasture data
    pasture = fba_w_sector.loc[fba_w_sector[sector_column].apply(lambda x: x[0:3]) == '112'].reset_index(drop=True)
    # concat crop and pasture
    fba_w_sector = pd.concat([pasture, crop], sort=True).reset_index(drop=True)

    # fill empty cells with NoneType
    fba_w_sector = replace_strings_with_NoneType(fba_w_sector)

    return fba_w_sector
