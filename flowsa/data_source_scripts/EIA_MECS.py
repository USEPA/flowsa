# EIA_MECS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8zip

import io
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.dataclean_functions import replace_strings_with_NoneType, replace_NoneType_with_empty_cells
import yaml

"""
MANUFACTURING ENERGY CONSUMPTION SURVEY (MECS)
https://www.eia.gov/consumption/manufacturing/data/2014/
Last updated: 8 Sept. 2020
"""

def eia_mecs_URL_helper(build_url, config, args):
    """
    Takes the build url and performs substitutions based on the EIA MECS year 
    and data tables of interest. Returns the finished url.
    """

    # initiate url list
    urls = []

    # for all tables listed in the source config file...
    for table in config['tables']:
        # start with build url
        url = build_url
        # replace '__year__' in build url
        url = url.replace('__year__', args['year'])
        # 2014 files are in .xlsx format; 2010 files are in .xls format
        if(args['year'] == '2010'):
            url = url[:-1]
        # replace '__table__' in build url
        url = url.replace('__table__', table)
        # add to list of urls
        urls.append(url)

    return urls


def eia_mecs_land_call(url, cbesc_response, args):
    # Convert response to dataframe
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='Table 9.1')
    df_raw_rse = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='RSE 9.1')
    if (args["year"] == "2014"):
        df_rse = pd.DataFrame(df_raw_rse.loc[12:93]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        # skip rows and remove extra rows at end of dataframe

        df_description.columns = ["NAICS Code(a)", "Subsector and Industry",
                                  "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                                  "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                                  "Approximate Number of All Buildings Onsite (counts)",
                                  "Average Number of Buildings Onsite per Establishment (counts)",
                                  "n8", "n9", "n10", "n11", "n12"]
        df_data.columns = ["NAICS Code(a)", "Subsector and Industry",
                           "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                           "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                           "Approximate Number of All Buildings Onsite (counts)",
                           "Average Number of Buildings Onsite per Establishment (counts)",
                           "n8", "n9", "n10", "n11", "n12"]
        df_rse.columns = ["NAICS Code(a)", "Subsector and Industry",
                          "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                          "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                          "Approximate Number of All Buildings Onsite (counts)",
                          "Average Number of Buildings Onsite per Establishment (counts)",
                          "n8", "n9", "n10", "n11", "n12"]

        #Drop unused columns
        df_description = df_description.drop(columns=["Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                                                      "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                                                      "Approximate Number of All Buildings Onsite (counts)",
                                                      "Average Number of Buildings Onsite per Establishment (counts)",
                                                      "n8", "n9", "n10", "n11", "n12"])

        df_data = df_data.drop(columns=["Subsector and Industry", "n8", "n9", "n10", "n11", "n12"])
        df_rse = df_rse.drop(columns=["Subsector and Industry", "n8", "n9", "n10", "n11", "n12"])
    else:
        df_rse = pd.DataFrame(df_raw_rse.loc[14:97]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description.columns = ["NAICS Code(a)", "Subsector and Industry",
                                  "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                                  "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                                  "Approximate Number of All Buildings Onsite (counts)",
                                  "Average Number of Buildings Onsite per Establishment (counts)"]
        df_data.columns = ["NAICS Code(a)", "Subsector and Industry",
                           "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                           "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                           "Approximate Number of All Buildings Onsite (counts)",
                           "Average Number of Buildings Onsite per Establishment (counts)"]
        df_rse.columns = ["NAICS Code(a)", "Subsector and Industry",
                          "Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                          "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                          "Approximate Number of All Buildings Onsite (counts)",
                          "Average Number of Buildings Onsite per Establishment (counts)"]
        # Drop unused columns
        df_description = df_description.drop(
            columns=["Approximate Enclosed Floorspace of All Buildings Onsite (million sq ft)",
                     "Establishments(b) (counts)", "Average Enclosed Floorspace per Establishment (sq ft)",
                     "Approximate Number of All Buildings Onsite (counts)",
                     "Average Number of Buildings Onsite per Establishment (counts)"])
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


def eia_mecs_land_parse(dataframe_list, args):
    df_array = []
    for dataframes in dataframe_list:

        dataframes = dataframes.rename(columns={'NAICS Code(a)': 'ActivityConsumedBy'})
        dataframes = dataframes.rename(columns={'Subsector and Industry': 'Description'})
        dataframes.loc[dataframes.Description == "Total", "ActivityConsumedBy"] = "31-33"
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
                unit.append(("p"))
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
    df['Description'] = df['Description'].apply(lambda x: x + ' Manufacturing' if not x.endswith('Manufacturing') else x)

    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = withdrawn_keyword
    df["Class"] = 'Land'
    df["SourceName"] = 'EIA_MECS_Land'
    df['Year'] = args["year"]
    df["Compartment"] = 'ground'
    df['MeasureofSpread'] = "RSE"
    df['Location'] = US_FIPS
    df['Unit'] = unit
    df = assign_fips_location_system(df, args['year'])
    df['FlowType'] = "ELEMENTARY_FLOW"

    # modify flowname
    df['FlowName'] = df['Description'] + ', ' + df['FlowName'].str.strip()

    return df


def eia_mecs_energy_call(url, mecs_response, args):
    """
    Takes the .xlsx or .xls file returned from the url call and reads it into a dataframe.
    Grabs data for each of the census regions and "unpivots" dataframe.
    Adds columns for census region, relative standard error, units.
    Concatenates census region data into master dataframe.
    Returns master dataframe containing data for all 4 census regions, plus U.S. totals.
    """

    ## load .yaml file containing information about each energy table
    ## (the .yaml includes information such as column names, units, and which rows to grab)
    filename = 'EIA_MECS_energy tables'
    sourcefile = datapath + filename + '.yaml'
    with open(sourcefile, 'r') as f:
        table_dict = yaml.safe_load(f)

    ## read raw data into dataframe
    ## (include both Sheet 1 (data) and Sheet 2 (relative standard errors))
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(mecs_response.content), sheet_name=0, header=None)
    df_raw_rse = pd.io.excel.read_excel(io.BytesIO(mecs_response.content), sheet_name=1, header=None)

    ## retrieve table name from cell A3 of Excel file
    table = df_raw_data.iloc[2][0]
    # drop the table description (retain only table name)
    table = table.split('    ')[0]

    ## for each of the census regions...
    ## - grab the appropriate rows and columns
    ## - add column names
    ## - "unpivot" dataframe from wide format to long format
    ## - add columns denoting census region, relative standard error, units
    ## - concatenate census region data into master dataframe
    df_data = pd.DataFrame()
    for region in table_dict[args['year']][table]['regions']:

        ## grab relevant columns
        ## (this is a necessary step because code was retaining some seemingly blank columns)
        # determine number of columns in table, based on number of column names
        num_cols = len(table_dict[args['year']][table]['col_names'])
        # keep only relevant columns
        df_raw_data = df_raw_data.iloc[:,0:num_cols]
        df_raw_rse = df_raw_rse.iloc[:,0:num_cols]

        ## grab relevant rows
        # get indices for relevant rows
        grab_rows = table_dict[args['year']][table]['regions'][region]
        grab_rows_rse = table_dict[args['year']][table]['rse_regions'][region]
        # keep only relevant rows
        df_data_region = pd.DataFrame(df_raw_data.loc[grab_rows[0]-1:grab_rows[1]-1]).reindex()
        df_rse_region = pd.DataFrame(df_raw_rse.loc[grab_rows_rse[0]-1:grab_rows_rse[1]-1]).reindex()

        # assign column names
        df_data_region.columns = table_dict[args['year']][table]['col_names']
        df_rse_region.columns = table_dict[args['year']][table]['col_names']

        # "unpivot" dataframe from wide format to long format
        # ('NAICS code' and 'Subsector and Industry' are identifier variables)
        # (all other columns are value variables)
        df_data_region = pd.melt(df_data_region,
                                 id_vars = table_dict[args['year']][table]['col_names'][0:2],
                                 value_vars = table_dict[args['year']][table]['col_names'][2:],
                                 var_name = 'FlowName',
                                 value_name = 'FlowAmount')
        df_rse_region = pd.melt(df_rse_region,
                                id_vars = table_dict[args['year']][table]['col_names'][0:2],
                                value_vars = table_dict[args['year']][table]['col_names'][2:],
                                var_name = 'FlowName',
                                value_name = 'Spread')

        # add census region
        df_data_region['Location'] = region

        # add relative standard error data
        df_data_region = pd.merge(df_data_region, df_rse_region)

        ## add units
        # if table name ends in 1, units must be extracted from flow names
        if table[-1] == '1':
            flow_name_array = df_data_region['FlowName'].str.split('\s+\|+\s')
            df_data_region['FlowName'] = flow_name_array.str[0]
            df_data_region['Unit'] = flow_name_array.str[1]
        # if table name ends in 2, units are 'trillion Btu'
        elif table[-1] == '2':
            df_data_region['Unit'] = 'Trillion Btu'
            df_data_region['FlowName'] = df_data_region['FlowName']

        data_type = table_dict[args['year']][table]['data_type']
        if data_type == 'nonfuel consumption':
            df_data_region['Class']='Other'
        elif data_type == 'fuel consumption':
            df_data_region['Class']='Energy'
        # remove extra spaces before 'Subsector and Industry' descriptions
        df_data_region['Subsector and Industry'] = df_data_region['Subsector and Industry'].str.lstrip(' ')

        # concatenate census region data with master dataframe
        df_data = pd.concat([df_data, df_data_region])

    return df_data


def eia_mecs_energy_parse(dataframe_list, args):

    from flowsa.common import assign_census_regions

    # concatenate dataframe list into single dataframe
    df = pd.concat(dataframe_list, sort=True)

    # rename columns to match standard flowbyactivity format
    df = df.rename(columns={'NAICS Code' : 'ActivityConsumedBy',
                            'Subsector and Industry' : 'Description'})
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
    # add hardcoded data
    df["SourceName"] = args['source']
    df["Compartment"] = None
    df['FlowType'] = 'TECHNOSPHERE_FLOWS'
    df['Year'] = args["year"]
    df['MeasureofSpread'] = "RSE"
    # assign location codes and location system
    df.loc[df['Location']=='Total United States','Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df = assign_census_regions(df)
    df.loc[df['Description'] == 'Total', 'ActivityConsumedBy'] = '31-33'


    # drop rows that reflect subtotals (only necessary in 2014)
    df.dropna(subset=['ActivityConsumedBy'], inplace=True)


    ## replace withheld/unavailable data
    # * = estimate is less than 0.5
    # W = withheld to avoid disclosing data for individual establishments
    # Q = withheld because relative standard error is greater than 50 percent
    # NA = not available
    df.loc[df['FlowAmount'] == '*', 'FlowAmount'] = None
    df.loc[df['FlowAmount'] == 'W', 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == 'Q', 'FlowAmount'] = withdrawn_keyword
    df.loc[df['FlowAmount'] == 'NA', 'FlowAmount'] = None
    # * = estimate is less than 0.5
    # W = withheld to avoid disclosing data for individual establishments
    # Q = withheld because relative standard error is greater than 50 percent
    # NA = not available
    # X = not defined because relative standard error corresponds to a value of zero
    # at least one 'empty' cell appears to contain a space
    df.loc[df['Spread'] == '*', 'Spread'] = None
    df.loc[df['Spread'] == 'W', 'Spread'] = withdrawn_keyword
    df.loc[df['Spread'] == 'Q', 'Spread'] = withdrawn_keyword
    df.loc[df['Spread'] == 'NA', 'Spread'] = None
    df.loc[df['Spread'] == 'X', 'Spread'] = None
    df.loc[df['Spread'] == ' ', 'Spread'] = None

    return df

def mecs_energy_fba_cleanup(fba, attr):

    fba = fba.loc[fba['Unit'] == 'MJ']

    # todo: subtract net elec from total

    return fba

def eia_mecs_energy_clean_allocation_fba_w_sec(df_w_sec, attr, method):
    """
    clean up eia_mecs_energy df with sectors by estimating missing data
    :param df_w_sec:
    :param attr:
    :param method:
    :return:
    """

    # drop rows where flowamount = 0, which drops supressed data
    df_w_sec = df_w_sec[df_w_sec['FlowAmount'] != 0].reset_index(drop=True)

    # estimate missing data
    df = determine_flows_requiring_disaggregation(df_w_sec, attr, method)

    # drop rows where flowamount = 0
    df2 = df[df['FlowAmount'] != 0].reset_index(drop=True)

    return df2


def mecs_land_fba_cleanup(fba):

    from flowsa.data_source_scripts.EIA_CBECS_Land import calculate_total_facility_land_area

    fba = fba[fba['FlowName'].str.contains('Approximate Enclosed Floorspace of All Buildings Onsite')]

    # calculate the land area in addition to building footprint
    fba = calculate_total_facility_land_area(fba)

    return fba

def mecs_land_fba_cleanup_for_land_2012_fbs(fba):
    """
    The 'land_national_2012' FlowBySector uses MECS 2014 data, set MECS year to 2012
    :param fba:
    :return:
    """

    fba = mecs_land_fba_cleanup(fba)

    fba['Year'] = 2012

    return fba


def mecs_land_clean_allocation_mapped_fba_w_sec(df, attr, method):
    """

    The mecs land dataset has varying levels of information for naics3-6. Iteratively determine which activities need allocated

    :param df: The mecs df with sectors after mapped to FEDEFL
    :param attr:
    :return:
    """

    df = determine_flows_requiring_disaggregation(df, attr, method)

    return df


def determine_flows_requiring_disaggregation(df_load, attr, method):
    """
    The MECS Land data provides FlowAmounts for NAICS3-6. We use BLS QCEW employment data to determine land use for
    different industries. To accurately estimate land use per industry, existing FlowAmounts for a particular NAICS
    level (NAICS6) for example, should be subtracted from the possible FlowAmounts for other NAICS6 that share the first
    5 digits. For Example, there is data for '311', '3112', and '311221' in the 2014 dataset. FlowAmounts for allocation
    by employment for NAICS6 are based on the provided '3112' FlowAmounts. However, since there is data at one NAICS6
    (311221), the FlowAmount for that NAICS6 should be subtracted from other NAICS6 to accurately depict the remaining
    'FlowAmount' that requires a secondary source (Employment data) for allocation.
    :param df_load:
    :return: A dataframe with a column 'disaggregate_flag', if '1', row requires secondary source to calculate
             FlowAmount, if '0' FlowAmount does not require modifications
    """

    from flowsa.mapping import add_sectors_to_flowbyactivity

    df_load = replace_NoneType_with_empty_cells(df_load)

    # modify to work with mapped vs unmapped dfs
    if 'Compartment' in df_load:
        c_col = 'Compartment'
        flow_col = 'FlowName'
    else:
        c_col = 'Context'
        flow_col = 'Flowable'

    # original df - subset
    # subset cols of original df
    dfo = df_load[['FlowAmount', flow_col, 'Location', 'SectorConsumedBy']]
    # min and max length
    min_length = min(df_load['SectorConsumedBy'].apply(lambda x: len(str(x))).unique())
    max_length = max(df_load['SectorConsumedBy'].apply(lambda x: len(str(x))).unique())
    # subset by sector length, creating a df
    for s in range(min_length, max_length+1):
        df_name = 'dfo_naics' + str(s)
        vars()[df_name] = dfo[dfo['SectorConsumedBy'].apply(lambda x: len(x) == s)].reset_index(drop=True)
        vars()[df_name] = vars()[df_name].assign(SectorMatch=vars()[df_name]['SectorConsumedBy'].apply(lambda x: x[:len(x) - 1]))
    # loop through the dfs, merging by sector match. If there is a match, subtract the value, \
    # if there is not a match, drop last digit in sectormatch, add row to the next df, and repeat
    df_merged = pd.DataFrame()
    df_not_merged = pd.DataFrame()
    for s in range(max_length, min_length, -1):
        df_name_1 = 'dfo_naics' + str(s-1)
        df_name_2 = 'dfo_naics' + str(s)
        # concat df 1 with df_not_merged
        df2 = pd.concat([vars()[df_name_2], df_not_merged])
        df2 = df2.rename(columns={'FlowAmount': 'SubtractFlow', 'SectorConsumedBy': 'Sector'})
        df_m = pd.merge(vars()[df_name_1][['FlowAmount', flow_col, 'Location', 'SectorConsumedBy']],
                        df2,
                        left_on=[flow_col, 'Location', 'SectorConsumedBy'],
                        right_on=[flow_col, 'Location', 'SectorMatch'],
                        indicator=True, how='outer')
        # subset by merge and append to appropriate df
        df_both = df_m[df_m['_merge'] == 'both']
        if len(df_both) != 0:
            # drop columns
            df_both1 = df_both.drop(columns=['Sector', 'SectorMatch', '_merge'])
            # aggregate before subtracting
            df_both2 = df_both1.groupby(['FlowAmount', flow_col, 'Location', 'SectorConsumedBy'], as_index=False)[["SubtractFlow"]].agg("sum")
            df_both3 = df_both2.assign(FlowAmount=df_both2['FlowAmount'] - df_both2['SubtractFlow'])
            df_both3 = df_both3.drop(columns=['SubtractFlow'])
            # drop rows where 0
            # df_both = df_both[df_both['FlowAmount'] != 0]
            df_merged = df_merged.append(df_both3, ignore_index=True)
        df_right = df_m[df_m['_merge'] == 'right_only']
        if len(df_right) != 0:
            df_right = df_right.drop(columns=['FlowAmount', 'SectorConsumedBy', '_merge'])
            df_right = df_right.rename(columns={'SubtractFlow': 'FlowAmount', 'Sector':'SectorConsumedBy'})
            # remove another digit from Sectormatch
            df_right = df_right.assign(SectorMatch=df_right['SectorConsumedBy'].apply(lambda x: x[:(s-2)]))
            # reorder
            df_right = df_right[['FlowAmount', flow_col, 'Location', 'SectorConsumedBy', 'SectorMatch']]
            df_not_merged = df_not_merged.append(df_right, ignore_index=True)
    # rename the flowamount column
    df_merged = df_merged.rename(columns={'FlowAmount': 'FlowAmountNew', 'SectorConsumedBy': 'ActivityConsumedBy'})
    # in the original df, drop sector columns re-add sectors, this time with sectors = 'aggregated'
    dfn = df_load.drop(columns=['SectorProducedBy', 'ProducedBySectorType', 'SectorConsumedBy', 'ConsumedBySectorType',
                                'SectorSourceName'])
    dfn = add_sectors_to_flowbyactivity(dfn, sectorsourcename=method['target_sector_source'],
                                        overwrite_sectorlevel='aggregated')
    # add column noting that these columns require an allocation ratio
    dfn = dfn.assign(disaggregate_flag=1)
    # create lists of sectors to drop
    list_original = df_load['ActivityConsumedBy'].drop_duplicates().tolist()
    # drop values in original df
    dfn2 = dfn[~dfn['SectorConsumedBy'].isin(list_original)].sort_values(['ActivityConsumedBy', 'SectorConsumedBy']).reset_index(drop=True)
    # drop the sectors that are duplicated by different naics being mapped to naics6
    if len(dfn2[dfn2.duplicated(subset=[flow_col, 'Location', 'SectorConsumedBy'], keep=False)]) > 0:
        dfn2.drop_duplicates(subset=[flow_col, 'Location', 'SectorConsumedBy'], keep='last', inplace=True)
    # want to allocate at NAICS6, so drop all other sectors
    dfn2 = dfn2[dfn2['SectorConsumedBy'].apply(lambda x: len(x) == 6)].reset_index(drop=True).sort_values(['SectorConsumedBy'])

    # merge revised flowamounts back with modified original df
    df_to_allocate = dfn2.merge(df_merged, how='left')
    # replace FlowAmount with newly calculated FlowAmount, which represents Flows that are currently unaccounted
    # for at NAICS6
    df_to_allocate['FlowAmount'] = np.where(df_to_allocate['FlowAmountNew'].notnull(),
                                             df_to_allocate['FlowAmountNew'], df_to_allocate['FlowAmount'])
    # drop rows where flow amount = 0 - flows are captured through other NAICS6
    df_to_allocate2 = df_to_allocate[df_to_allocate['FlowAmount'] != 0].drop(columns='FlowAmountNew').reset_index(drop=True)

    # merge the original df with modified
    # add column to original df for disaggregate_flag
    df_load = df_load.assign(disaggregate_flag=0)

    # concat the two dfs and sort
    df_c = pd.concat([df_load, df_to_allocate2], ignore_index=True).sort_values(['SectorConsumedBy']).reset_index(drop=True)

    df_c = replace_strings_with_NoneType(df_c).sort_values(['SectorConsumedBy'])

    return df_c

