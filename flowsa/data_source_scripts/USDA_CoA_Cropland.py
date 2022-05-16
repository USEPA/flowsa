# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions used to import and parse USDA Census of Ag Cropland data
"""

import json
import numpy as np
import pandas as pd
from flowsa.allocation import allocate_by_sector, \
    equally_allocate_parent_to_child_naics, equal_allocation
from flowsa.common import WITHDRAWN_KEYWORD, fba_wsec_default_grouping_fields
from flowsa.dataclean import replace_NoneType_with_empty_cells, \
    replace_strings_with_NoneType
from flowsa.flowbyfunctions import assign_fips_location_system, \
    sector_aggregation, sector_disaggregation, sector_ratios, \
    load_fba_w_standardized_units, \
    equally_allocate_suppressed_parent_to_child_naics
from flowsa.location import US_FIPS, abbrev_us_state
from flowsa.sectormapping import add_sectors_to_flowbyactivity
from flowsa.validation import compare_df_units


def CoA_Cropland_URL_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        for y in config['sector_levels']:
            # at national level, remove the text string calling for
            # state acronyms
            if x == 'NATIONAL':
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__secLevel__", y)
                url = url.replace("&state_alpha=__stateAlpha__", "")
                if y == "ECONOMICS":
                    url = url.replace(
                        "AREA%20HARVESTED&statisticcat_desc=AREA%20IN%20"
                        "PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc="
                        "AREA%20BEARING%20%26%20NON-BEARING",
                        "AREA&statisticcat_desc=AREA%20OPERATED")
                else:
                    url = url.replace("&commodity_desc=AG%20LAND&"
                                      "commodity_desc=FARM%20OPERATIONS", "")
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
                            "AREA%20HARVESTED&statisticcat_desc=AREA%20IN%20"
                            "PRODUCTION&statisticcat_desc=TOTAL&"
                            "statisticcat_desc=AREA%20BEARING%20%26%20NON-BEARING",
                            "AREA&statisticcat_desc=AREA%20OPERATED")
                    else:
                        url = url.replace("&commodity_desc=AG%20LAND&commodity_"
                                          "desc=FARM%20OPERATIONS", "")
                    urls.append(url)
    return urls


def coa_cropland_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    cropland_json = json.loads(resp.text)
    df_cropland = pd.DataFrame(data=cropland_json["data"])
    return df_cropland


def coa_cropland_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # specify desired data based on domain_desc
    df = df[~df['domain_desc'].isin(
        ['ECONOMIC CLASS', 'FARM SALES', 'IRRIGATION STATUS', 'CONCENTRATION',
         'ORGANIC STATUS', 'NAICS CLASSIFICATION', 'PRODUCERS'])]
    df = df[df['statisticcat_desc'].isin(
        ['AREA HARVESTED', 'AREA IN PRODUCTION', 'AREA BEARING & NON-BEARING',
         'AREA', 'AREA OPERATED', 'AREA GROWN'])]
    # drop rows that subset data into farm sizes (ex. 'area harvested:
    # (1,000 to 1,999 acres)
    df = df[~df['domaincat_desc'].str.contains(
        ' ACRES')].reset_index(drop=True)
    # drop Descriptions that contain certain phrases, as these data are
    # included in other categories
    df = df[~df['short_desc'].str.contains(
        'FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP')]
    # drop Descriptions that contain certain phrases - only occur in
    # AG LAND data
    df = df[~df['short_desc'].str.contains(
        'INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE')].reset_index(drop=True)
    # Many crops are listed as their own commodities as well as grouped
    # within a broader category (for example, orange
    # trees are also part of orchards). As this dta is not needed,
    # takes up space, and can lead to double counting if
    # included, want to drop these unused columns
    # subset dataframe into the 5 crop types and land in farms and drop rows
    # crop totals: drop all data
    # field crops: don't want certain commodities and don't
    # want detailed types of wheat, cotton, or sunflower
    df_fc = df[df['group_desc'] == 'FIELD CROPS']
    df_fc = df_fc[~df_fc['commodity_desc'].isin(
        ['GRASSES', 'GRASSES & LEGUMES, OTHER', 'LEGUMES', 'HAY', 'HAYLAGE'])]
    df_fc = df_fc[~df_fc['class_desc'].str.contains(
        'SPRING|WINTER|TRADITIONAL|OIL|PIMA|UPLAND', regex=True)]
    # fruit and tree nuts: only want a few commodities
    df_ftn = df[df['group_desc'] == 'FRUIT & TREE NUTS']
    df_ftn = df_ftn[df_ftn['commodity_desc'].isin(
        ['BERRY TOTALS', 'ORCHARDS'])]
    df_ftn = df_ftn[df_ftn['class_desc'].isin(['ALL CLASSES'])]
    # horticulture: only want a few commodities
    df_h = df[df['group_desc'] == 'HORTICULTURE']
    df_h = df_h[df_h['commodity_desc'].isin(
        ['CUT CHRISTMAS TREES', 'SHORT TERM WOODY CROPS'])]
    # vegetables: only want a few commodities
    df_v = df[df['group_desc'] == 'VEGETABLES']
    df_v = df_v[df_v['commodity_desc'].isin(['VEGETABLE TOTALS'])]
    # only want ag land and farm operations in farms & land & assets
    df_fla = df[df['group_desc'] == 'FARMS & LAND & ASSETS']
    df_fla = df_fla[df_fla['short_desc'].str.contains(
        "AG LAND|FARM OPERATIONS")]
    # drop the irrigated acreage in farms (want the irrigated harvested acres)
    df_fla = df_fla[
        ~((df_fla['domaincat_desc'] == 'AREA CROPLAND, HARVESTED: (ANY)') &
          (df_fla['domain_desc'] == 'AREA CROPLAND, HARVESTED') &
          (df_fla['short_desc'] == 'AG LAND, IRRIGATED - ACRES'))]
    # concat data frames
    df = pd.concat([df_fc, df_ftn, df_h, df_v, df_fla],
                   sort=False).reset_index(drop=True)
    # drop unused columns
    df = df.drop(columns=['agg_level_desc', 'location_desc', 'state_alpha',
                          'sector_desc', 'country_code', 'begin_code',
                          'watershed_code', 'reference_period_desc',
                          'asd_desc', 'county_name', 'source_desc',
                          'congr_district_code', 'asd_code', 'week_ending',
                          'freq_desc', 'load_time', 'zip_5', 'watershed_desc',
                          'region_desc', 'state_ansi', 'state_name',
                          'country_name', 'county_ansi', 'end_code',
                          'group_desc'])
    # create FIPS column by combining existing columns
    df.loc[df['county_code'] == '', 'county_code'] = '000'
    df['Location'] = df['state_fips_code'] + df['county_code']
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS

    # address non-NAICS classification data
    # use info from other columns to determine flow name
    df.loc[:, 'FlowName'] = df['statisticcat_desc'] + ', ' + \
                            df['prodn_practice_desc']
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(
        ", ALL PRODUCTION PRACTICES", "", regex=True)
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(
        ", IN THE OPEN", "", regex=True)
    # want to included "harvested" in the flowname when "harvested" is
    # included in the class_desc
    df['FlowName'] = np.where(df['class_desc'].str.contains(', HARVESTED'),
                              df['FlowName'] + " HARVESTED", df['FlowName'])
    # reorder
    df['FlowName'] = np.where(df['FlowName'] == 'AREA, IRRIGATED HARVESTED',
                              'AREA HARVESTED, IRRIGATED', df['FlowName'])
    # combine column information to create activity
    # information, and create two new columns for activities
    df['Activity'] = df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + \
                     df['util_practice_desc']  # drop this column later
    # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(
        ", ALL CLASSES", "", regex=True)
    # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(
        ", ALL UTILIZATION PRACTICES", "", regex=True)
    df['ActivityProducedBy'] = np.where(
        df["unit_desc"] == 'OPERATIONS', df["Activity"], None)
    df['ActivityConsumedBy'] = np.where(
        df["unit_desc"] == 'ACRES', df["Activity"], None)

    # rename columns to match flowbyactivity format
    df = df.rename(columns={"Value": "FlowAmount", "unit_desc": "Unit",
                            "year": "Year", "CV (%)": "Spread",
                            "short_desc": "Description"})
    # drop remaining unused columns
    df = df.drop(columns=['Activity', 'class_desc', 'commodity_desc',
                          'domain_desc', 'state_fips_code', 'county_code',
                          'statisticcat_desc', 'prodn_practice_desc',
                          'domaincat_desc', 'util_practice_desc'])
    # modify contents of units column
    df.loc[df['Unit'] == 'OPERATIONS', 'Unit'] = 'p'
    # modify contents of flowamount column, "D" is supressed data,
    # "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)
    # USDA CoA 2017 states that (H) means CV >= 99.95,
    # therefore replacing with 99.95 so can convert column to int
    # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None
    df.loc[df['Spread'] == "(D)", 'Spread'] = WITHDRAWN_KEYWORD
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # Add hardcoded data
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland"
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 2

    return df


def coa_nonirrigated_cropland_fba_cleanup(fba, **kwargs):
    """
    Cleanup coa cropland data for nonirrigated crops
    :param fba: df, COA when using non-irrigated data
    :return: df, COA nonirrigated data, modified
    """

    # when include 'area harvested' and 'area in production' in
    # single dataframe, which is necessary to include woody crops,
    # 'vegetable totals' are double counted
    fba = fba[~((fba['FlowName'] == 'AREA IN PRODUCTION') &
                (fba['ActivityConsumedBy'] == 'VEGETABLE TOTALS'))]

    # When using a mix of flow names, drop activities for ag land (naics 11)
    # and ag land, cropland, harvested (naics 111),because published values
    # for harvested cropland do not include data for vegetables, woody crops,
    # berries. Values for sectors 11 and 111 will be aggregated from the
    # dataframe later
    fba = fba[~fba['ActivityConsumedBy'].isin(
        ['AG LAND', 'AG LAND, CROPLAND, HARVESTED'])].reset_index(drop=True)

    return fba


def disaggregate_coa_cropland_to_6_digit_naics(
        fba_w_sector, attr, method, **kwargs):
    """
    Disaggregate usda coa cropland to naics 6
    :param fba_w_sector: df, CoA cropland data, FBA format with sector columns
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: dictionary, FBS method yaml
    :param kwargs: dictionary, arguments that might be required for other
        functions. Currently includes data source name.
    :return: df, CoA cropland with disaggregated NAICS sectors
    """

    # define the activity and sector columns to base modifications on
    # these definitions will vary dependent on class type
    sector_col = 'SectorConsumedBy'

    # drop rows without assigned sectors
    fba_w_sector = fba_w_sector[
        ~fba_w_sector[sector_col].isna()].reset_index(drop=True)

    # modify the flowamounts related to the 6 naics 'orchards' are mapped to
    fba_w_sector = equal_allocation(fba_w_sector)

    # use ratios of usda 'land in farms' to determine animal use of
    # pasturelands at 6 digit naics
    fba_w_sector = disaggregate_pastureland(
        fba_w_sector, attr, method, year=attr['allocation_source_year'],
        sector_column=sector_col,
        download_FBA_if_missing=kwargs['download_FBA_if_missing'])

    # use ratios of usda 'harvested cropland' to determine missing 6 digit
    # naics
    fba_w_sector = disaggregate_cropland(
        fba_w_sector, attr, method, year=attr['allocation_source_year'],
        sector_column=sector_col, download_FBA_if_missing=kwargs[
            'download_FBA_if_missing'])

    return fba_w_sector


def disaggregate_coa_cropland_to_6_digit_naics_for_water_withdrawal(
        fba_w_sector_load, attr, method, **kwargs):
    """
    Disaggregate usda coa cropland to naics 6
    :param fba_w_sector_load: df, CoA cropland data, FBA format with sector
    columns
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: dictionary, FBS method yaml
    :param kwargs: dictionary, arguments that might be required for other
           functions. Currently includes data source name.
    :return: df, CoA cropland with disaggregated NAICS sectors
    """

    # define the activity and sector columns to base modifications on
    # these definitions will vary dependent on class type
    sector_col = 'SectorConsumedBy'

    # drop rows without assigned sectors
    fba_w_sector = fba_w_sector_load[~fba_w_sector_load[sector_col].isna()]\
        .reset_index(drop=True)

    # modify the flowamounts related to the 6 naics 'orchards' are mapped to
    fba_w_sector = equal_allocation(fba_w_sector)

    # todo: add back in once suppression fxn modified to accept non-naics
    #  like activities and mixed level final naics (naics6 and naics7)
    # then estimate any suppressed data by equally allocating parent to
    # child naics
    # groupcols = list(fba_w_sector3.select_dtypes(
    #     include=['object', 'int']).columns)
    # fba_w_sector = equally_allocate_suppressed_parent_to_child_naics(
    #     fba_w_sector, method, 'SectorConsumedBy', groupcols)

    # When using irrigated cropland, aggregate sectors to cropland and total
    # ag land. Doing this because published values for irrigated harvested
    # cropland do not include the water use for vegetables, woody crops,
    # berries.
    fba_w_sector = fba_w_sector[~fba_w_sector['ActivityConsumedBy'].isin(
        ['AG LAND', 'AG LAND, CROPLAND, HARVESTED'])].reset_index(drop=True)

    # use ratios of usda 'land in farms' to determine animal use of
    # pasturelands at 6 digit naics
    fba_w_sector = disaggregate_pastureland(
        fba_w_sector, attr, method, year=attr['allocation_source_year'],
        sector_column=sector_col, download_FBA_if_missing=kwargs[
            'download_FBA_if_missing'], parameter_drop=['1125'])

    # use ratios of usda 'harvested cropland' to determine missing 6 digit
    # naics
    fba_w_sector = disaggregate_cropland(
        fba_w_sector, attr, method, year=attr['allocation_source_year'],
        sector_column=sector_col, download_FBA_if_missing=kwargs[
            'download_FBA_if_missing'])

    return fba_w_sector


def disaggregate_pastureland(fba_w_sector, attr, method, year,
                             sector_column, download_FBA_if_missing, **kwargs):
    """
    The USDA CoA Cropland irrigated pastureland data only links
    to the 3 digit NAICS '112'. This function uses state
    level CoA 'Land in Farms' to allocate the county level acreage data to
    6 digit NAICS.
    :param fba_w_sector: df, the CoA Cropland dataframe after linked to sectors
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: string, methodname
    :param year: str, year of data being disaggregated
    :param sector_column: str, the sector column on which to make df
                          modifications (SectorProducedBy or SectorConsumedBy)
    :param download_FBA_if_missing: bool, if True will attempt to load
        FBAS used in generating the FBS from remote server prior to
        generating if file not found locally
    :return: df, the CoA cropland dataframe with disaggregated pastureland data
    """

    # tmp drop NoneTypes
    fba_w_sector = replace_NoneType_with_empty_cells(fba_w_sector)

    # subset the coa data so only pastureland
    p = fba_w_sector.loc[fba_w_sector[sector_column].apply(
        lambda x: x[0:3]) == '112'].reset_index(drop=True)
    if len(p) != 0:
        # add temp loc column for state fips
        p = p.assign(Location_tmp=p['Location'].apply(lambda x: x[0:2]))

        # load usda coa cropland naics
        df_f = load_fba_w_standardized_units(
            datasource='USDA_CoA_Cropland_NAICS', year=year, flowclass='Land',
            download_FBA_if_missing=download_FBA_if_missing)
        # subset to land in farms data
        df_f = df_f[df_f['FlowName'] == 'FARM OPERATIONS']
        # subset to rows related to pastureland
        df_f = df_f.loc[df_f['ActivityConsumedBy'].apply(
            lambda x: x[0:3]) == '112']
        # drop rows with "&'
        df_f = df_f[~df_f['ActivityConsumedBy'].str.contains('&')]
        if 'parameter_drop' in kwargs:
            # drop aquaculture because pastureland not used for aquaculture
            # drop any activities at a more aggregated sector level because
            # will need to be reaggregated after dropping a parameter to
            # accurately calculate the allocation ratios
            drop_list = [sub[ : -1] for sub in  kwargs['parameter_drop']]
            drop_list = drop_list + kwargs['parameter_drop']
            df_f = df_f[~df_f['ActivityConsumedBy'].isin(drop_list)]
        # create sector columns
        df_f = add_sectors_to_flowbyactivity(
            df_f, sectorsourcename=method['target_sector_source'])
        # estimate suppressed data by equal allocation
        df_f = equally_allocate_suppressed_parent_to_child_naics(
            df_f, method, 'SectorConsumedBy', fba_wsec_default_grouping_fields)
        # create proportional ratios
        group_cols = [e for e in fba_wsec_default_grouping_fields if
                      e not in ('ActivityProducedBy', 'ActivityConsumedBy')]
        df_f = allocate_by_sector(df_f, attr, 'proportional', group_cols)
        # tmp drop NoneTypes
        df_f = replace_NoneType_with_empty_cells(df_f)
        # drop naics = '11
        df_f = df_f[df_f[sector_column] != '11']
        # drop 000 in location
        df_f = df_f.assign(Location=df_f['Location'].apply(lambda x: x[0:2]))

        # check units before merge
        compare_df_units(p, df_f)
        # merge the coa pastureland data with land in farm data
        df = p.merge(df_f[[sector_column, 'Location', 'FlowAmountRatio']],
                     how='left', left_on="Location_tmp", right_on="Location")
        # multiply the flowamount by the flowratio
        df.loc[:, 'FlowAmount'] = df['FlowAmount'] * df['FlowAmountRatio']
        # drop columns and rename
        df = df.drop(columns=['Location_tmp', sector_column + '_x',
                              'Location_y', 'FlowAmountRatio'])
        df = df.rename(columns={sector_column + '_y': sector_column,
                                "Location_x": 'Location'})

        # drop rows where sector = 112 and then concat with
        # original fba_w_sector
        fba_w_sector = fba_w_sector[fba_w_sector[sector_column].apply(
            lambda x: x[0:3]) != '112'].reset_index(drop=True)
        fba_w_sector = pd.concat([fba_w_sector, df]).reset_index(drop=True)

        # fill empty cells with NoneType
        fba_w_sector = replace_strings_with_NoneType(fba_w_sector)

    return fba_w_sector


def disaggregate_cropland(fba_w_sector, attr, method, year,
                          sector_column, download_FBA_if_missing):
    """
    In the event there are 4 (or 5) digit naics for cropland
    at the county level, use state level harvested cropland to
    create ratios
    :param fba_w_sector: df, CoA cropland data, FBA format with sector columns
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: string, method name
    :param year: str, year of data
    :param sector_column: str, the sector column on which to make
        df modifications (SectorProducedBy or SectorConsumedBy)
    :param download_FBA_if_missing: bool, if True will attempt to
        load FBAS used in generating the FBS from remote server prior to
        generating if file not found locally
    :return: df, CoA cropland data disaggregated
    """

    # tmp drop NoneTypes
    fba_w_sector = replace_NoneType_with_empty_cells(fba_w_sector)

    # drop pastureland data
    crop = fba_w_sector.loc[fba_w_sector[sector_column].apply(
        lambda x: x[0:3]) != '112'].reset_index(drop=True)
    # drop sectors < 4 digits
    crop = crop[crop[sector_column].apply(
        lambda x: len(x) > 3)].reset_index(drop=True)
    # create tmp location
    crop = crop.assign(Location_tmp=crop['Location'].apply(lambda x: x[0:2]))

    # load the relevant state level harvested cropland by naics
    naics = load_fba_w_standardized_units(
        datasource="USDA_CoA_Cropland_NAICS", year=year,
        flowclass='Land', download_FBA_if_missing=download_FBA_if_missing)
    # subset the harvested cropland by naics
    naics = naics[naics['FlowName'] ==
                  'AG LAND, CROPLAND, HARVESTED'].reset_index(drop=True)
    # drop the activities that include '&'
    naics = naics[~naics['ActivityConsumedBy'].str.contains(
        '&')].reset_index(drop=True)
    # add sectors
    naics = add_sectors_to_flowbyactivity(
        naics, sectorsourcename=method['target_sector_source'])
    # estimate suppressed data by equally allocating parent to child naics
    naics = equally_allocate_suppressed_parent_to_child_naics(
        naics, method, 'SectorConsumedBy', fba_wsec_default_grouping_fields)

    # aggregate sectors to create any missing naics levels
    naics2 = sector_aggregation(naics)
    # add missing naics5/6 when only one naics5/6 associated with a naics4
    naics3 = sector_disaggregation(naics2)
    # create ratios
    naics4 = sector_ratios(naics3, sector_column)
    # create temporary sector column to match the two dfs on
    naics4 = naics4.assign(
        Location_tmp=naics4['Location'].apply(lambda x: x[0:2]))
    # tmp drop Nonetypes
    naics4 = replace_NoneType_with_empty_cells(naics4)

    # check units in prep for merge
    compare_df_units(crop, naics4)
    # for loop through naics lengths to determine
    # naics 4 and 5 digits to disaggregate
    for i in range(4, 6):
        # subset df to sectors with length = i and length = i + 1
        crop_subset = crop.loc[crop[sector_column].apply(
            lambda x: i + 1 >= len(x) >= i)]
        crop_subset = crop_subset.assign(
            Sector_tmp=crop_subset[sector_column].apply(lambda x: x[0:i]))
        # if duplicates drop all rows
        df = crop_subset.drop_duplicates(
            subset=['Location', 'Sector_tmp'],
            keep=False).reset_index(drop=True)
        # drop sector temp column
        df = df.drop(columns=["Sector_tmp"])
        # subset df to keep the sectors of length i
        df_subset = df.loc[df[sector_column].apply(lambda x: len(x) == i)]
        # subset the naics df where naics length is i + 1
        naics_subset = \
            naics4.loc[naics4[sector_column].apply(
                lambda x: len(x) == i + 1)].reset_index(drop=True)
        naics_subset = naics_subset.assign(
            Sector_tmp=naics_subset[sector_column].apply(lambda x: x[0:i]))
        # merge the two df based on locations
        df_subset = pd.merge(
            df_subset, naics_subset[[sector_column, 'FlowAmountRatio',
                                     'Sector_tmp', 'Location_tmp']],
            how='left', left_on=[sector_column, 'Location_tmp'],
            right_on=['Sector_tmp', 'Location_tmp'])
        # create flow amounts for the new NAICS based on the flow ratio
        df_subset.loc[:, 'FlowAmount'] = \
            df_subset['FlowAmount'] * df_subset['FlowAmountRatio']
        # drop rows of 0 and na
        df_subset = df_subset[df_subset['FlowAmount'] != 0]
        df_subset = df_subset[
            ~df_subset['FlowAmount'].isna()].reset_index(drop=True)
        # drop columns
        df_subset = df_subset.drop(
            columns=[sector_column + '_x', 'FlowAmountRatio', 'Sector_tmp'])
        # rename columns
        df_subset = df_subset.rename(
            columns={sector_column + '_y': sector_column})
        # tmp drop Nonetypes
        df_subset = replace_NoneType_with_empty_cells(df_subset)
        # add new rows of data to crop df
        crop = pd.concat([crop, df_subset]).reset_index(drop=True)

    # clean up df
    crop = crop.drop(columns=['Location_tmp'])

    # equally allocate any further missing naics
    crop = equally_allocate_parent_to_child_naics(crop, method)

    # pasture data
    pasture = \
        fba_w_sector.loc[fba_w_sector[sector_column].apply(
            lambda x: x[0:3]) == '112'].reset_index(drop=True)
    # concat crop and pasture
    fba_w_sector = pd.concat([pasture, crop]).reset_index(drop=True)

    # fill empty cells with NoneType
    fba_w_sector = replace_strings_with_NoneType(fba_w_sector)

    return fba_w_sector
