# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used across flowsa"""

import shutil
import os
import yaml
import requests
import requests_ftp
import pandas as pd
import numpy as np
import pycountry
from urllib.parse import urlsplit
from dotenv import load_dotenv
from esupy.processed_data_mgmt import create_paths_if_missing
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields, \
    flow_by_activity_wsec_fields, flow_by_activity_mapped_wsec_fields, \
    activity_fields
from flowsa.settings import datapath, MODULEPATH, logoutputpath, \
    sourceconfigpath, log, flowbysectormethodpath


# Sets default Sector Source Name
SECTOR_SOURCE_NAME = 'NAICS_2012_Code'
flow_types = ['ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW', 'WASTE_FLOW']

US_FIPS = "00000"

fips_number_key = {"national": 0,
                   "state": 2,
                   "county": 5}

sector_level_key = {"NAICS_2": 2,
                    "NAICS_3": 3,
                    "NAICS_4": 4,
                    "NAICS_5": 5,
                    "NAICS_6": 6}

# withdrawn keyword changed to "none" over "W"
# because unable to run calculation functions with text string
WITHDRAWN_KEYWORD = np.nan


def load_api_key(api_source):
    """
    Loads an API Key from "API_Keys.env" file using the
    'api_name' defined in the FBA source config file. The '.env' file contains
    the users personal API keys. The user must register with this
    API and get the key and manually add to "API_Keys.env"

    See wiki for how to get an api:
    https://github.com/USEPA/flowsa/wiki/Using-FLOWSA#api-keys

    :param api_source: str, name of source, like 'BEA' or 'Census'
    :return: the users API key as a string
    """
    load_dotenv(f'{MODULEPATH}API_Keys.env', verbose=True)
    key = os.getenv(api_source)
    if key is None:
        log.error(f"Key file {api_source} not found. See github wiki for help "
                  "https://github.com/USEPA/flowsa/wiki/Using-FLOWSA#api-keys")
    return key


def load_crosswalk(crosswalk_name):
    """
    Load NAICS crosswalk between the years 2007, 2012, 2017
    :return: df, NAICS crosswalk over the years
    """

    cw_dict = {'sector_timeseries': 'NAICS_Crosswalk_TimeSeries',
               'sector_length': 'NAICS_2012_Crosswalk',
               'household': 'Household_SectorCodes',
               'government': 'Government_SectorCodes',
               'BEA': 'NAICS_to_BEA_Crosswalk'
               }

    fn = cw_dict.get(crosswalk_name)

    cw = pd.read_csv(f'{datapath}{fn}.csv', dtype="str")
    return cw


def return_bea_codes_used_as_naics():
    """

    :return: list of BEA codes used as NAICS
    """
    cw_list = []
    for cw in ['household', 'government']:
        df = load_crosswalk(cw)
        cw_list.append(df)
    # concat data into single dataframe
    cw = pd.concat(cw_list, sort=False)
    code_list = cw['Code'].drop_duplicates().values.tolist()
    return code_list


def load_yaml_dict(filename, flowbytype=None):
    """
    Load the information in 'source_catalog.yaml'
    :return: dictionary containing all information in source_catalog.yaml
    """
    if filename == 'source_catalog':
        folder = datapath
    else:
        if flowbytype == 'FBA':
            folder = sourceconfigpath
        elif flowbytype == 'FBS':
            folder = flowbysectormethodpath
        else:
            raise KeyError('Must specify either \'FBA\' or \'FBS\'')
    yaml_path = folder + filename + '.yaml'

    try:
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
    except IOError:
        log.error('%s method file not found', flowbytype)

    # Allow for .yaml files to recursively inherit other .yaml files. Keys in
    # children will overwrite the same key from a parent.
    inherits = config.get('inherits_from')
    while inherits:
        yaml_path = folder + inherits + '.yaml'
        with open(yaml_path, 'r') as f:
            parent = yaml.safe_load(f)

        # Check for common keys and log a warning if any are found
        common_keys = [k for k in config if k in parent]
        if common_keys:
            log.warning(f'Keys {common_keys} from parent file {yaml_path} '
                        f'were overwritten by child file.')

        # Update inheritance information before updating the parent dict
        inherits = parent.get('inherits_from')
        parent.update(config)
        config = parent

    return config


def load_values_from_literature_citations_config():
    """
    Load the config file that contains information on where the
    values from the literature come from
    :return: dictionary of the values from the literature information
    """
    sfile = (f'{datapath}bibliographyinfo/'
             f'values_from_literature_source_citations.yaml')
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config


def load_fbs_methods_additional_fbas_config():
    """
    Load the config file that contains information on where the
    values from the literature come from
    :return: dictionary of the values from the literature information
    """
    sfile = f'{datapath}bibliographyinfo/fbs_methods_additional_fbas.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config


def load_functions_loading_fbas_config():
    """
    Load the config file that contains information on where the
    values from the literature come from
    :return: dictionary of the values from the literature information
    """
    sfile = datapath + 'bibliographyinfo/functions_loading_fbas.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config


def create_fill_na_dict(flow_by_fields):
    """
    Dictionary for how to fill nan in different column types
    :param flow_by_fields: list of columns
    :return: dictionary for how to fill missing values by dtype
    """
    fill_na_dict = {}
    for k, v in flow_by_fields.items():
        if v[0]['dtype'] == 'str':
            fill_na_dict[k] = ""
        elif v[0]['dtype'] == 'int':
            fill_na_dict[k] = 0
        elif v[0]['dtype'] == 'float':
            fill_na_dict[k] = 0
    return fill_na_dict


def get_flow_by_groupby_cols(flow_by_fields):
    """
    Return groupby columns for a type of dataframe
    :param flow_by_fields: dictionary
    :return: list, column names
    """
    groupby_cols = []
    for k, v in flow_by_fields.items():
        if v[0]['dtype'] == 'str':
            groupby_cols.append(k)
        elif v[0]['dtype'] == 'int':
            groupby_cols.append(k)
    if flow_by_fields == flow_by_activity_fields:
        # Do not use description for grouping
        groupby_cols.remove('Description')
    return groupby_cols


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]
fbs_activity_fields = [activity_fields['ProducedBy'][1]['flowbysector'],
                       activity_fields['ConsumedBy'][1]['flowbysector']]
fba_fill_na_dict = create_fill_na_dict(flow_by_activity_fields)
fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)
fbs_collapsed_fill_na_dict = create_fill_na_dict(
    flow_by_sector_collapsed_fields)
fba_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_activity_fields)
fba_mapped_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_activity_mapped_fields)
fba_mapped_wsec_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_activity_mapped_wsec_fields)
fbs_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_sector_fields)
fbs_grouping_fields_w_activities = (
    fbs_default_grouping_fields + (['ActivityProducedBy',
                                    'ActivityConsumedBy']))
fbs_collapsed_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_sector_collapsed_fields)
fba_wsec_default_grouping_fields = get_flow_by_groupby_cols(
    flow_by_activity_wsec_fields)


def read_stored_FIPS(year='2015'):
    """
    Read fips based on year specified, year defaults to 2015
    :param year: str, '2010', '2013', or '2015', default year is 2015
        because most recent year of FIPS available
    :return: df, FIPS for specified year
    """

    FIPS_df = pd.read_csv(datapath + "FIPS_Crosswalk.csv", header=0, dtype=str)
    # subset columns by specified year
    df = FIPS_df[["State", "FIPS_" + year, "County_" + year]]
    # rename columns to drop data year
    df.columns = ['State', 'FIPS', 'County']
    # sort df
    df = df.sort_values(['FIPS']).reset_index(drop=True)

    return df


def getFIPS(state=None, county=None, year='2015'):
    """
    Pass a state or state and county name to get the FIPS.

    :param state: str. A US State Name or Puerto Rico, any case accepted
    :param county: str. A US county
    :param year: str. '2010', '2013', '2015', default year is 2015
    :return: str. A five digit FIPS code
    """
    FIPS_df = read_stored_FIPS(year)

    # default code
    code = None

    if county is None:
        if state is not None:
            state = clean_str_and_capitalize(state)
            code = FIPS_df.loc[(FIPS_df["State"] == state)
                               & (FIPS_df["County"].isna()), "FIPS"]
        else:
            log.error("To get state FIPS, state name must be passed in "
                      "'state' param")
    else:
        if state is None:
            log.error("To get county FIPS, state name must be passed in "
                      "'state' param")
        else:
            state = clean_str_and_capitalize(state)
            county = clean_str_and_capitalize(county)
            code = FIPS_df.loc[(FIPS_df["State"] == state)
                               & (FIPS_df["County"] == county), "FIPS"]
    if code.empty:
        log.error("No FIPS code found")
    else:
        code = code.values[0]

    return code


def apply_county_FIPS(df, year='2015', source_state_abbrev=True):
    """
    Applies FIPS codes by county to dataframe containing columns with State
    and County
    :param df: dataframe must contain columns with 'State' and 'County', but
        not 'Location'
    :param year: str, FIPS year, defaults to 2015
    :param source_state_abbrev: True or False, the state column uses
        abbreviations
    :return dataframe with new column 'FIPS', blanks not removed
    """
    # If using 2 letter abbrevations, map to state names
    if source_state_abbrev:
        df['State'] = df['State'].map(abbrev_us_state)
    df['State'] = df.apply(lambda x: clean_str_and_capitalize(x.State),
                           axis=1)
    df['County'] = df.apply(lambda x: clean_str_and_capitalize(x.County),
                            axis=1)

    # Pull and merge FIPS on state and county
    mapping_FIPS = get_county_FIPS(year)
    df = df.merge(mapping_FIPS, how='left')

    # Where no county match occurs, assign state FIPS instead
    mapping_FIPS = get_state_FIPS()
    mapping_FIPS.drop(columns=['County'], inplace=True)
    df = df.merge(mapping_FIPS, left_on='State', right_on='State', how='left')
    df['Location'] = df['FIPS_x'].where(df['FIPS_x'].notnull(), df['FIPS_y'])
    df.drop(columns=['FIPS_x', 'FIPS_y'], inplace=True)

    return df


def update_geoscale(df, to_scale):
    """
    Updates df['Location'] based on specified to_scale
    :param df: df, requires Location column
    :param to_scale: str, target geoscale
    :return: df, with 5 digit fips
    """
    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        df.loc[:, 'Location'] = df['Location'].apply(lambda x: str(x[0:2]))
        # pad zeros
        df.loc[:, 'Location'] = df['Location'].apply(lambda x:
                                                     x.ljust(3 + len(x), '0')
                                                     if len(x) < 5 else x)
    elif to_scale == 'national':
        df.loc[:, 'Location'] = US_FIPS
    return df


def clean_str_and_capitalize(s):
    """
    Trim whitespace, modify string so first letter capitalized.
    :param s: str
    :return: str, formatted
    """
    if s.__class__ == str:
        s = s.strip()
        s = s.lower()
        s = s.capitalize()
    return s


def capitalize_first_letter(string):
    """
    Capitalize first letter of words
    :param string: str
    :return: str, modified
    """
    return_string = ""
    split_array = string.split(" ")
    for s in split_array:
        return_string = return_string + " " + s.capitalize()
    return return_string.strip()


def get_state_FIPS(year='2015'):
    """
    Filters FIPS df for state codes only
    :param year: str, year of FIPS, defaults to 2015
    :return: FIPS df with only state level records
    """

    fips = read_stored_FIPS(year)
    fips = fips.drop_duplicates(subset='State')
    fips = fips[fips['State'].notnull()]
    return fips


def get_county_FIPS(year='2015'):
    """
    Filters FIPS df for county codes only
    :param year: str, year of FIPS, defaults to 2015
    :return: FIPS df with only county level records
    """
    fips = read_stored_FIPS(year)
    fips = fips.drop_duplicates(subset='FIPS')
    fips = fips[fips['County'].notnull()]
    return fips


def get_all_state_FIPS_2(year='2015'):
    """
    Gets a subset of all FIPS 2 digit codes for states
    :param year: str, year of FIPS, defaults to 2015
    :return: df with 'State' and 'FIPS_2' cols
    """

    state_fips = get_state_FIPS(year)
    state_fips.loc[:, 'FIPS_2'] = state_fips['FIPS'].apply(lambda x: x[0:2])
    state_fips = state_fips[['State', 'FIPS_2']]
    return state_fips


# From https://gist.github.com/rogerallen/1583593
# removed non US states, PR, MP, VI
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state = {abbr: state for state, abbr in us_state_abbrev.items()}


def get_region_and_division_codes():
    """
    Load the Census Regions csv
    :return: pandas df of census regions
    """
    df = pd.read_csv(f"{datapath}Census_Regions_and_Divisions.csv",
                     dtype="str")
    return df


def assign_census_regions(df_load):
    """
    Assign census regions as a LocationSystem
    :param df_load: fba or fbs
    :return: df with census regions as LocationSystem
    """
    # load census codes
    census_codes_load = get_region_and_division_codes()
    census_codes = census_codes_load[
        census_codes_load['LocationSystem'] == 'Census_Region']

    # merge df with census codes
    df = df_load.merge(census_codes[['Name', 'Region']],
                       left_on=['Location'], right_on=['Name'], how='left')
    # replace Location value
    df['Location'] = np.where(~df['Region'].isnull(),
                              df['Region'], df['Location'])

    # modify LocationSystem
    # merge df with census codes
    df = df.merge(census_codes[['Region', 'LocationSystem']],
                  left_on=['Region'], right_on=['Region'], how='left')
    # replace Location value
    df['LocationSystem_x'] = np.where(~df['LocationSystem_y'].isnull(),
                                      df['LocationSystem_y'],
                                      df['LocationSystem_x'])

    # drop census columns
    df = df.drop(columns=['Name', 'Region', 'LocationSystem_y'])
    df = df.rename(columns={'LocationSystem_x': 'LocationSystem'})

    return df


def call_country_code(country):
    """
    use pycountry to call on 3 digit iso country code
    :param country: str, name of country
    :return: str, ISO code
    """
    return pycountry.countries.get(name=country).numeric


def convert_fba_unit(df):
    """
    Convert unit to standard
    :param df: df, FBA flowbyactivity
    :return: df, FBA with standarized units
    """
    # Convert Water units 'Bgal/d' and 'Mgal/d' to Mgal
    days_in_year = 365
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'Bgal/d',
        df['FlowAmount'] * 1000 * days_in_year,
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Bgal/d',
                                 'Mgal', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'Mgal/d',
        df['FlowAmount'] * days_in_year,
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal/d',
                                 'Mgal', df['Unit'])

    # Convert Land unit 'Thousand Acres' to 'Acres
    acres_in_thousand_acres = 1000
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'Thousand Acres',
        df['FlowAmount'] * acres_in_thousand_acres,
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Thousand Acres',
                                 'Acres', df['Unit'])

    # Convert Energy unit "Quadrillion Btu" to MJ
    mj_in_btu = .0010550559
    # 1 Quad = .0010550559 x 10^15
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'Quadrillion Btu',
        df['FlowAmount'] * mj_in_btu * (10 ** 15),
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Quadrillion Btu',
                                 'MJ', df['Unit'])

    # Convert Energy unit "Trillion Btu" to MJ
    # 1 Tril = .0010550559 x 10^14
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'Trillion Btu',
        df['FlowAmount'] * mj_in_btu * (10 ** 14),
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Trillion Btu',
                                 'MJ', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'million Cubic metres/year',
        df['FlowAmount'] * 264.172,
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'million Cubic metres/year',
                                 'Mgal', df['Unit'])

    # Convert mass units (LB or TON) to kg
    ton_to_kg = 907.185
    lb_to_kg = 0.45359
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'TON',
        df['FlowAmount'] * ton_to_kg,
        df['FlowAmount'])
    df.loc[:, 'FlowAmount'] = np.where(
        df['Unit'] == 'LB',
        df['FlowAmount'] * lb_to_kg,
        df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where((df['Unit'] == 'TON') | (df['Unit'] == 'LB'),
                                 'kg', df['Unit'])

    return df


def get_flowsa_base_name(filedirectory, filename, extension):
    """
    If filename does not match filename within flowsa due to added extensions
    onto the filename, cycle through
    name, dropping strings after each underscore until the name is found
    :param filedirectory: string, path to directory
    :param filename: string, name of original file searching for
    :param extension: string, type of file, such as "yaml" or "py"
    :return: string, corrected file path name
    """
    # If a file does not exist, modify file name, dropping portion after last
    # underscore. Repeat this process until the file name exists or no
    # underscores are left.
    while '_' in filename:
        if os.path.exists(f"{filedirectory}{filename}.{extension}"):
            break
        filename, _ = filename.rsplit('_', 1)

    return filename


def rename_log_file(filename, fb_meta):
    """
    Rename the log file saved to local directory using df meta for df
    :param filename: str, name of dataset
    :param fb_meta: metadata for parquet
    :return: modified log file name
    """
    # original log file name - all log statements
    log_file = f'{logoutputpath}{"flowsa.log"}'
    # generate new log name
    new_log_name = (f'{logoutputpath}{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'.log')
    # create log directory if missing
    create_paths_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)
    # original log file name - validation
    log_file = f'{logoutputpath}{"validation_flowsa.log"}'
    # generate new log name
    new_log_name = (f'{logoutputpath}{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'_validation.log')
    # create log directory if missing
    create_paths_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)


def return_true_source_catalog_name(sourcename):
    """
    Drop any extensions on source name until find the name in source catalog
    """
    while (load_yaml_dict('source_catalog').get(sourcename) is None) & ('_' in sourcename):
        sourcename = sourcename.rsplit("_", 1)[0]
    return sourcename


def check_activities_sector_like(sourcename_load):
    """
    Check if the activities in a df are sector-like,
    if cannot find the sourcename in the source catalog, drop extensions on the
    source name
    """
    sourcename = return_true_source_catalog_name(sourcename_load)

    try:
        sectorLike = load_yaml_dict('source_catalog')[sourcename]['sector-like_activities']
    except KeyError:
        log.error(f'%s or %s not found in {datapath}source_catalog.yaml',
                  sourcename_load, sourcename)

    return sectorLike


def str2bool(v):
    """
    Convert string to boolean
    :param v: string
    :return: boolean
    """
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    else:
        return False
