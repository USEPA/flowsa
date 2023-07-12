# EIA_MECS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8zip

"""
MANUFACTURING ENERGY CONSUMPTION SURVEY (MECS)
https://www.eia.gov/consumption/manufacturing/data/2014/
Last updated: 8 Sept. 2020
"""
import io

import pandas as pd
import numpy as np
import flowsa
from flowsa.location import US_FIPS, get_region_and_division_codes
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowsa_log import log
from flowsa.flowby import FlowByActivity
from flowsa.flowbyfunctions import assign_fips_location_system, sector_aggregation
from flowsa.data_source_scripts.EIA_CBECS_Land import \
    calculate_total_facility_land_area


def eia_mecs_URL_helper(*, build_url, config, year, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into
    Flow-By-Activity format
    """
    # initiate url list
    urls = []

    # for all tables listed in the source config file...
    for table in config['tables']:
        # start with build url
        url = build_url
        # replace '__year__' in build url
        url = url.replace('__year__', year)
        # 2014 files are in .xlsx format; 2010 files are in .xls format
        if year == '2010':
            url = url[:-1]
        # replace '__table__' in build url
        url = url.replace('__table__', table)
        # add to list of urls
        urls.append(url)

    return urls


def eia_mecs_land_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    df_raw_data = pd.read_excel(io.BytesIO(resp.content),
                                sheet_name='Table 9.1')
    df_raw_rse = pd.read_excel(io.BytesIO(resp.content),
                               sheet_name='RSE 9.1')
    if year == "2014":
        # skip rows and remove extra rows at end of dataframe
        df_rse = pd.DataFrame(df_raw_rse.loc[12:93]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        # pull first 7 columns
        df_rse = df_rse.iloc[:, 0:7]
        df_data = df_data.iloc[:, 0:7]
        df_description = df_description.iloc[:, 0:7]

        df_description.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All "
             "Buildings Onsite (million sq ft)",
             "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]
        df_data.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All "
             "Buildings Onsite (million sq ft)",
             "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]
        df_rse.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All Buildings Onsite "
             "(million sq ft)", "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]

        # Drop unused columns
        df_description = df_description.drop(
            columns=["Approximate Enclosed Floorspace of All Buildings Onsite "
                     "(million sq ft)", "Establishments(b) (counts)",
                     "Average Enclosed Floorspace per Establishment (sq ft)",
                     "Approximate Number of All Buildings Onsite (counts)",
                     "Average Number of Buildings Onsite per Establishment "
                     "(counts)"])

        df_data = df_data.drop(columns=["Subsector and Industry"])
        df_rse = df_rse.drop(columns=["Subsector and Industry"])
    else:
        df_rse = pd.DataFrame(df_raw_rse.loc[14:97]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All "
             "Buildings Onsite (million sq ft)", "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]
        df_data.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All Buildings Onsite "
             "(million sq ft)", "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]
        df_rse.columns = \
            ["NAICS Code(a)", "Subsector and Industry",
             "Approximate Enclosed Floorspace of All Buildings Onsite "
             "(million sq ft)", "Establishments(b) (counts)",
             "Average Enclosed Floorspace per Establishment (sq ft)",
             "Approximate Number of All Buildings Onsite (counts)",
             "Average Number of Buildings Onsite per Establishment (counts)"]
        # Drop unused columns
        df_description = df_description.drop(
            columns=["Approximate Enclosed Floorspace of All Buildings Onsite "
                     "(million sq ft)", "Establishments(b) (counts)",
                     "Average Enclosed Floorspace per Establishment (sq ft)",
                     "Approximate Number of All Buildings Onsite (counts)",
                     "Average Number of Buildings Onsite per Establishment "
                     "(counts)"])
        df_data = df_data.drop(columns=["Subsector and Industry"])
        df_rse = df_rse.drop(columns=["Subsector and Industry"])

    df_data = df_data.melt(id_vars=["NAICS Code(a)"],
                           var_name="FlowName",
                           value_name="FlowAmount")
    df_rse = df_rse.melt(id_vars=["NAICS Code(a)"],
                         var_name="FlowName",
                         value_name="Spread")

    df = pd.merge(df_data, df_rse)
    df = pd.merge(df, df_description)

    return df


def eia_mecs_land_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df_array = []
    for dataframes in df_list:

        dataframes = dataframes.rename(
            columns={'NAICS Code(a)': 'ActivityConsumedBy'})
        dataframes = dataframes.rename(
            columns={'Subsector and Industry': 'Description'})
        dataframes.loc[
            dataframes.Description == "Total", "ActivityConsumedBy"] = "31-33"
        unit = []
        for index, row in dataframes.iterrows():
            if row["FlowName"] == "Establishments(b) (counts)":
                row["FlowName"] = "Establishments (counts)"
            flow_name_str = row["FlowName"]
            flow_name_array = flow_name_str.split("(")
            row["FlowName"] = flow_name_array[0]
            unit_text = flow_name_array[1]
            unit_text_array = unit_text.split(")")
            if unit_text_array[0] == "counts":
                unit.append("p")
            else:
                unit.append(unit_text_array[0])
            ACB = row["ActivityConsumedBy"]
            ACB_str = str(ACB).strip()
            row["ActivityConsumedBy"] = ACB_str
        df_array.append(dataframes)
    df = pd.concat(df_array, sort=False)

    # trim whitespace associated with Activity
    df['Description'] = df['Description'].str.strip()

    # add manufacturing to end of description if missing
    df['Description'] = df['Description'].apply(
        lambda x: x + ' Manufacturing' if not
        x.endswith('Manufacturing') else x)

    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df["Class"] = 'Land'
    df["SourceName"] = 'EIA_MECS_Land'
    df['Year'] = year
    df["Compartment"] = 'ground'
    df['MeasureofSpread'] = "RSE"
    df['Location'] = US_FIPS
    df['Unit'] = unit
    df = assign_fips_location_system(df, year)
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # modify flowname
    df['FlowName'] = df['FlowName'].str.strip()

    return df


def eia_mecs_energy_call(*, resp, year, config, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: df, response from url call
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: pandas dataframe of original source data
    """
    # load dictionary containing information about each energy table
    # (the .yaml includes information such as column names, units, and
    # which rows to grab)
    table_dict = config['table_dict']

    # read raw data into dataframe
    # (include both Sheet 1 (data) and Sheet 2 (relative standard errors))
    df_raw_data = pd.read_excel(io.BytesIO(resp.content),
                                sheet_name=0, header=None)
    df_raw_rse = pd.read_excel(io.BytesIO(resp.content),
                               sheet_name=1, header=None)

    # retrieve table name from cell A3 of Excel file
    table = df_raw_data.iloc[2][0]
    # drop the table description (retain only table name)
    table = table.split('    ')[0]
    table = table.split('  ')[0]

    # for each of the census regions...
    # - grab the appropriate rows and columns
    # - add column names
    # - "unpivot" dataframe from wide format to long format
    # - add columns denoting census region, relative standard error, units
    # - concatenate census region data into master dataframe
    df_data = pd.DataFrame()

    for region in table_dict[year][table]['regions']:

        # grab relevant columns
        # (this is a necessary step because code was retaining some seemingly
        # blank columns) determine number of columns in table, based on
        # number of column names
        num_cols = len(table_dict[year][table]['col_names'])
        # keep only relevant columns
        df_raw_data = df_raw_data.iloc[:, 0:num_cols]
        df_raw_rse = df_raw_rse.iloc[:, 0:num_cols]

        # grab relevant rows
        # get indices for relevant rows
        grab_rows = table_dict[year][table]['regions'][region]
        grab_rows_rse = table_dict[year][table]['rse_regions'][region]
        # keep only relevant rows
        df_data_region = pd.DataFrame(
            df_raw_data.loc[grab_rows[0] - 1:grab_rows[1] - 1]).reindex()
        df_rse_region = pd.DataFrame(df_raw_rse.loc[
            grab_rows_rse[0] - 1:grab_rows_rse[1] - 1]).reindex()

        # assign column names
        # if table name ends in 1, the column names are pulled from the table dict unchanged
        if table[-1] == '1':
            df_data_region.columns = table_dict[year][table]['col_names']
            df_rse_region.columns = table_dict[year][table]['col_names']
        # if table name ends in 2, the units must be stripped from the column names listed in the table dict
        if table[-1] in ['2', '0', '5', '6']:
            df_data_region.columns = [name.split(' | ', 2)[0] for name in table_dict[year][table]['col_names']]
            df_rse_region.columns = [name.split(' | ', 2)[0] for name in table_dict[year][table]['col_names']]

        if table[-1] == '5':
            major_name = ""
            df_data_region = df_data_region.dropna()
            for index, row in df_data_region.iterrows():
                name = ""
                minor_name = ""
                name = row["Energy Source"]
                if "    " in str(name):
                    minor_name = name.replace('    ', '')
                    if "Biomass Total" == minor_name:
                        major_name = minor_name
                        minor_name = ""
                    name = str(major_name) + " " + str(minor_name)
                    df_data_region.at[index, 'Energy Source'] = name
                else:
                    major_name = row["Energy Source"]
            df_data_region = df_data_region.rename(
                columns={'Energy Source': 'FlowName', 'Total First Use': 'FlowAmount'})
            df_data_region['NAICS Code'] = 'All Purposes'
            major_name = ""
            df_rse_region = df_rse_region.dropna()
            for index, row in df_rse_region.iterrows():
                name = ""
                minor_name = ""
                name = row["Energy Source"]
                if "    " in str(name):
                    minor_name = name.replace('    ', '')
                    if "Biomass Total" == minor_name:
                        major_name = minor_name
                        minor_name = ""
                    name = str(major_name) + " " + str(minor_name)
                    df_rse_region.at[index, 'Energy Source'] = name
                else:
                    major_name = row["Energy Source"]
            df_rse_region = df_rse_region.rename(
                columns={'Energy Source': 'FlowName', 'Total First Use': 'Spread'})
        else:
            # "unpivot" dataframe from wide format to long format
            # ('NAICS code' and 'Subsector and Industry' are identifier variables)
            # (all other columns are value variables)
            df_data_region = pd.melt(
                df_data_region,
                id_vars=df_data_region.columns[0:2],
                value_vars=df_data_region.columns[2:],
                var_name='FlowName', value_name='FlowAmount')
            df_rse_region = pd.melt(
                df_rse_region,
                id_vars=df_rse_region.columns[0:2],
                value_vars=df_rse_region.columns[2:],
                var_name='FlowName', value_name='Spread')

        # add relative standard error data
        df_data_region = pd.merge(df_data_region, df_rse_region)
        # add census region
        df_data_region['Location'] = region
        df_data_region['Table Name'] = table

        # add units
        # if table name ends in 1, units must be extracted from flow names
        if table[-1] == '1':
            flow_name_array = df_data_region['FlowName'].str.split('\s+\|+\s')
            df_data_region['Unit'] = flow_name_array.str[1]
            df_data_region['FlowName'] = flow_name_array.str[0]
        # if table name ends in 2, units are 'trillion Btu'
        elif table[-1] in ['2', '5', '6']:
            df_data_region['Unit'] = 'Trillion Btu'
            if table[-3] == '7': #7_2
                df_data_region['Unit'] = 'USD / million btu'
        elif table[-1] == '0':
            df_data_region['Unit'] = 'million USD'

        data_type = table_dict[year][table]['data_type']
        if data_type == 'nonfuel consumption':
            df_data_region['Class'] = 'Other'
        elif data_type == 'fuel consumption':
            df_data_region['Class'] = 'Energy'
        elif data_type == 'money':
            df_data_region['Class'] = 'Money'

        # remove extra spaces before 'Subsector and Industry' descriptions
        if table[-1] != '5':
            df_data_region['Subsector and Industry'] = \
                df_data_region['Subsector and Industry'].str.lstrip(' ')
        else:
            df_data_region['Subsector and Industry'] = "Total"

        # concatenate census region data with master dataframe
        df_data = pd.concat([df_data, df_data_region])

    return df_data


def eia_mecs_energy_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param source: source
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    from flowsa.location import assign_census_regions

    # concatenate dataframe list into single dataframe
    df = pd.concat(df_list, sort=True)

    # rename columns to match standard flowbyactivity format
    df = df.rename(columns={'NAICS Code': 'ActivityConsumedBy',
                            'Table Name': 'Description'})
    df.loc[df['Subsector and Industry'] == 'Total', 'ActivityConsumedBy'] = '31-33'
    df = df.drop(columns='Subsector and Industry')
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
    # add hardcoded data
    df["SourceName"] = source
    df["Compartment"] = None
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Year'] = year
    df['MeasureofSpread'] = "RSE"
    # assign location codes and location system
    df.loc[df['Location'] == 'Total United States', 'Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df = assign_census_regions(df)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # drop rows that reflect subtotals (only necessary in 2014)
    df = df.dropna(subset=['ActivityConsumedBy'])

    df = df.assign(
        FlowAmount=df.FlowAmount.mask(df.FlowAmount.str.isnumeric() == False,
                                      np.nan),
        Suppressed=df.FlowAmount.where(df.FlowAmount.str.isnumeric() == False,
                                       np.nan),
        Spread=df.Spread.mask(df.Spread.str.isnumeric() == False, np.nan)
    )

    return df


def estimate_suppressed_mecs_energy(
        fba: FlowByActivity,
        **kwargs
    ) -> FlowByActivity:
    '''
    Rough first pass at an estimation method, for testing purposes. This
    will drop rows with 'D' or 'Q' values, on the grounds that as far as I can
    tell we don't have any more information for them than we do for any
    industry without its own line item in the MECS anyway. '*' is for value
    less than 0.5 Trillion Btu and will be assumed to be 0.25 Trillion Btu
    '''
    if 'Suppressed' not in fba.columns:
        log.warning('The current MECS dataframe does not contain data '
                    'on estimation method and so suppressed data will '
                    'not be assessed.')
        return fba
    dropped = fba.query('Suppressed not in ["D", "Q"]')
    unsuppressed = dropped.assign(
        FlowAmount=dropped.FlowAmount.mask(dropped.Suppressed == '*', 0.25)
    )

    return unsuppressed.drop(columns='Suppressed')

def clean_mecs_energy_fba(fba: FlowByActivity, **_) -> FlowByActivity:
    '''
    This function helps address the unique structure of the EIA MECS dataset.
    The MECS dataset contains rows at various levels of aggregation between
    NAICS-3 and NAICS-6 (inclusive). Each aggregated row contains the total
    for that level of aggregation, even if data are also reported for a less
    aggregated subset of those industries. For example:

    ActivityConsumedBy | FlowAmount | ...
    -------------------------------------
    311                | 110        |
    3112               |  65        |
    311221             |  55        |

    where the 110 reported for 311 includes the 65 reported for 3112, which
    includes the 55 reported for 211221. If we do not address this issue, there
    will be double counting. Additionally, if we are trying to disaggregate
    to the NAICS-6 level, all three rows shown above will be mapped to NAICS-6
    311221 (with the first wo rows also being mapped to several other NAICS-6
    codes as well). We will then over attribute the (double-counted) flows to
    those industries and groups of industries for which more specific detail
    is provided.

    This function addresses the double counting issue. For each aggregated
    industry group, all descendant (less aggregated) industries or industry
    groups for which detailed information is given are subtracted from the
    aggregated total. Using the example from above:

    ActivityConsumedBy | FlowAmount | ...
    -------------------------------------
    311                |  45        |
    3112               |  10        |
    311221             |  55        |

    Additionally, this function adds a column called "descendants", which for
    each industry holds all the descendant industries or industry groups that
    have detailed information provided in the dataset. After mapping to
    industries, but before attribution is performed, this column is used by the
    clean_mapped_mecs_energy_fba function to drop any row that is mapped
    from an aggregated industry group to a less aggregated industry or industry
    group THAT HAS DETAILED INFORMATION GIVEN IN THE MECS (and therefore has
    its own row already) to avoid the over-attribution issue.
    Again using the previous example:

    ActivityConsumedBy | FlowAmount | descendants | ...
    ---------------------------------------------------
    311                |  45        | 3112 311221 |
    3112               |  10        | 311221      |
    311221             |  55        |             |

    Note that this function is not useful if the desired aggregation level is
    NAICS-2. In such a case, the MECS dataset can be filtered to include only
    the rows with ActivityConsumedBy == "31-33", then disaggregated to 31, 32,
    33 using another dataset (such as the QCEW).
    '''
    mecs = (
        fba
        .query('ActivityConsumedBy != "31-33"')
        .assign(descendants='')
    )

    for level in [5, 4, 3]:
        descendants = (
            mecs
            .drop(columns='descendants')
            .query(f'ActivityConsumedBy.str.len() > {level}')
            .assign(
                parent=lambda x: x.ActivityConsumedBy.str.slice(stop=level)
            )
            .groupby(['Flowable', 'Location', 'parent'])
            .agg({'FlowAmount': 'sum', 'ActivityConsumedBy': ' '.join})
            .reset_index()
            .rename(columns={'ActivityConsumedBy': 'descendants',
                             'FlowAmount': 'descendant_flows',
                             'parent': 'ActivityConsumedBy'})
        )

        mecs = (
            mecs
            .merge(descendants,
                   how='left',
                   on=['Flowable', 'Location', 'ActivityConsumedBy'],
                   suffixes=(None, '_y'))
            .fillna({'descendant_flows': 0, 'descendants_y': ''})
            .assign(
                descendants=lambda x: x.descendants.mask(x.descendants == '',
                                                         x.descendants_y),
                FlowAmount=lambda x: (x.FlowAmount - x.descendant_flows).mask(
                    x.FlowAmount - x.descendant_flows < 0, 0)
            )
            .drop(columns=['descendant_flows', 'descendants_y'])
        )

    return mecs.drop(columns=['descendants'])


def clean_mapped_mecs_energy_fba(fba: FlowByActivity, **_) -> FlowByActivity:
    '''
    This function finishes handling the over-attribution issue described in
    the documentation for clean_mecs_energy_fba by dropping any row in the
    MECS dataset which has been mapped to an industry or industry group which
    is a subset (strict or otherwise) of an industry group listed in the
    descendants columns. So, if 311 and 3112 both appear in the MECS datset,
    3112 will be listed as a descendant of 311 and this function will therefore
    drop a row mapping 311 to 311221 (since more detailed information on 3112,
    which contains 311221, is provided). If 31122 and 311221 do not appear in
    the dataset, a row mapping 3112 to 311221 will not be dropped, since no
    more detailed information on 311221 is given. Further attribution/
    disaggregation should be done using another datatset such as the QCEW.
    '''
    mecs = (
        fba
        .assign(to_keep=fba.apply(
            lambda x: not any([str(x.SectorConsumedBy).startswith(d)
                               for d in x.descendants.split()]),
            axis='columns'
        ))
        .query('to_keep')
        .drop(columns=['descendants', 'to_keep'])
    )

    return mecs




def clean_mapped_mecs_energy_fba_to_state(
        fba: FlowByActivity, **_
    ) -> FlowByActivity:
    """
    clean_fba_w_sec fxn that replicates clean_mapped_mecs_energy_fba but
    also updates regions to states for state models.
    """
    fba = clean_mapped_mecs_energy_fba(fba)
    fba = update_regions_to_states(fba)
    return fba


def update_regions_to_states(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    Propogates regions to all states to enable for use in state methods.
    Allocates sectors across states based on employment.
    clean_allocation_fba_w_sec fxn
    """
    fba_load = fba.copy()
    log.info('Updating census regions to states')

    region_map = get_region_and_division_codes()
    region_map = region_map[['Region','State_FIPS']].drop_duplicates()
    region_map.loc[:, 'State_FIPS'] = (
        region_map['State_FIPS'].apply(lambda x:
                                       x.ljust(3 + len(x), '0')
                                       if len(x) < 5 else x))

    # Allocate MECS based on employment FBS
    hlp = flowsa.getFlowBySector(
        methodname=fba.config.get('attribution_source'),
        download_FBS_if_missing=True)

    # To match the various sector resolution of MECS, generate employment
    # dataset for all NAICS resolution by aggregating
    hlp = sector_aggregation(hlp)

    # For each region, generate ratios across states for a given sector
    hlp = hlp.merge(region_map, how = 'left', left_on = 'Location',
                    right_on = 'State_FIPS')
    hlp['Allocation'] = hlp['FlowAmount']/hlp.groupby(
        ['Region', 'SectorProducedBy']).FlowAmount.transform('sum')

    fba = pd.merge(
        fba.rename(columns={'Location': 'Region'}),
        (hlp[['Region', 'Location', 'SectorProducedBy', 'Allocation']]
         .rename(columns={'SectorProducedBy': 'SectorConsumedBy'})),
        how='left', on=['Region', 'SectorConsumedBy'])
    fba = (fba.assign(FlowAmount = lambda x: x['FlowAmount'] * x['Allocation'])
              .assign(LocationSystem = 'FIPS_2015')
              .drop(columns=['Allocation', 'Region'])
              )

    # Rest group_id and group_total
    fba = (
        fba
        .drop(columns=['group_id', 'group_total'])
        .reset_index(drop=True).reset_index()
        .rename(columns={'index': 'group_id'})
        .assign(group_total=fba.FlowAmount)
    )

    # Check for data loss
    if (abs(1-(sum(fba['FlowAmount']) /
               sum(fba_load['FlowAmount'])))) > 0.0005:
        log.warning('Data loss upon census region mapping')

    return fba


def mecs_land_fba_cleanup(fba, **_):
    """
    Modify the EIA MECS Land FBA
    :param fba: df, EIA MECS Land FBA format
    :return: df, EA MECS Land FBA
    """
    fba2 = clean_mecs_energy_fba(fba)

    # calculate the land area in addition to building footprint
    fba3 = calculate_total_facility_land_area(fba2)

    return fba3
