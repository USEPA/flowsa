# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used across flowsa"""

import shutil
import os
import yaml
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from esupy.processed_data_mgmt import create_paths_if_missing
import flowsa.flowsa_yaml as flowsa_yaml
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields, \
    flow_by_activity_wsec_fields, flow_by_activity_mapped_wsec_fields, \
    activity_fields
from flowsa.settings import datapath, MODULEPATH, logoutputpath, \
    sourceconfigpath, log, flowbysectormethodpath

# set version number for use in FBA and FBS output naming schemas, needs to be updated with setup.py
PKG_VERSION_NUMBER = '0.3.2'

try:
    MODULEPATH = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    MODULEPATH = 'flowsa/'

datapath = MODULEPATH + 'data/'
sourceconfigpath = datapath + 'flowbyactivitymethods/'
crosswalkpath = datapath + 'activitytosectormapping/'
flowbysectormethodpath = datapath + 'flowbysectormethods/'
flowbysectoractivitysetspath = datapath + 'flowbysectoractivitysets/'
externaldatapath = datapath + 'external_data/'

datasourcescriptspath = MODULEPATH + 'data_source_scripts/'

paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/flowsa")
outputpath = paths.local_path.replace('\\', '/') + '/'
fbaoutputpath = outputpath + 'FlowByActivity/'
fbsoutputpath = outputpath + 'FlowBySector/'
biboutputpath = outputpath + 'Bibliography/'
logoutputpath = outputpath + 'Log/'

DEFAULT_DOWNLOAD_IF_MISSING = False

# paths to scripts
scriptpath = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + \
             '/scripts/'
scriptsFBApath = scriptpath + 'FlowByActivity_Datasets/'


# define 4 logs, one for general information, one for major validation logs that are
# also included in the general info log, one for very specific validation that is only
# included in the validation log, and a console printout that includes general and
# validation, but not detailed validation
create_paths_if_missing(logoutputpath)

# format for logging .txt generated
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

# create loggers
# general logger
log = logging.getLogger('allLog')
log.setLevel(logging.DEBUG)
log.propagate=False
# log.propagate=False
# general validation logger
vLog = logging.getLogger('validationLog')
vLog.setLevel(logging.DEBUG)
vLog.propagate=False
# detailed validation logger
vLogDetailed = logging.getLogger('validationLogDetailed')
vLogDetailed.setLevel(logging.DEBUG)
vLogDetailed.propagate=False

# create handlers
# create handler for overall logger
log_fh = logging.FileHandler(logoutputpath+'flowsa.log', mode='w')
log_fh.setFormatter(formatter)
# create handler for general validation information
vLog_fh = logging.FileHandler(logoutputpath+'validation_flowsa.log', mode='w')
vLog_fh.setFormatter(formatter)
# create console handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

# add handlers to various loggers
# general logger
log.addHandler(ch) # print to console
log.addHandler(log_fh)
vLog.addHandler(log_fh)
# validation logger
vLog.addHandler(ch) # print to console
vLog.addHandler(vLog_fh)
vLogDetailed.addHandler(vLog_fh)

# Sets default Sector Source Name
SECTOR_SOURCE_NAME = 'NAICS_2012_Code'
flow_types = ['ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW', 'WASTE_FLOW']

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
               'sector_name': 'NAICS_2012_Names',
               'household': 'Household_SectorCodes',
               'government': 'Government_SectorCodes',
               'BEA': 'NAICS_to_BEA_Crosswalk'
               }

    fn = cw_dict.get(crosswalk_name)

    cw = pd.read_csv(f'{datapath}{fn}.csv', dtype="str")
    return cw


def load_sector_length_cw_melt():
    cw_load = load_crosswalk('sector_length')
    cw_melt = cw_load.melt(var_name="SectorLength", value_name='Sector'
                           ).drop_duplicates().reset_index(drop=True)
    cw_melt = cw_melt.dropna().reset_index(drop=True)
    cw_melt['SectorLength'] = cw_melt['SectorLength'].str.replace(
        'NAICS_', "")
    cw_melt['SectorLength'] = pd.to_numeric(cw_melt['SectorLength'])

    cw_melt = cw_melt[['Sector', 'SectorLength']]

    return cw_melt


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


def load_yaml_dict(filename, flowbytype=None, filepath=None):
    """
    Load the information in a yaml file, from source_catalog, or FBA,
    or FBS files
    :return: dictionary containing all information in yaml
    """
    if filename == 'source_catalog':
        folder = datapath
    else:
        # first check if a filepath for the yaml is specified, as is the
        # case with FBS method files located outside FLOWSA
        if filepath is not None:
            log.info(f'Loading {filename} from {filepath}')
            folder = filepath
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
            config = flowsa_yaml.load(f, filepath)
    except IOError:
        log.error(f'{flowbytype} method file not found')
        raise
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
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))


def get_region_and_division_codes():
    """
    Load the Census Regions csv
    :return: pandas df of census regions
    """
    df = pd.read_csv(datapath + "Census_Regions_and_Divisions.csv", dtype="str")
    return df


def assign_census_regions(df_load):
    """
    Assign census regions as a LocationSystem
    :param df_load: fba or fbs
    :return: df with census regions as LocationSystem
    """
    # load census codes
    census_codes_load = get_region_and_division_codes()
    census_codes = census_codes_load[census_codes_load['LocationSystem'] == 'Census_Region']

    # merge df with census codes
    df = df_load.merge(census_codes[['Name', 'Region']],
                       left_on=['Location'], right_on=['Name'], how='left')
    # replace Location value
    df['Location'] = np.where(~df['Region'].isnull(), df['Region'], df['Location'])

    # modify LocationSystem
    # merge df with census codes
    df = df.merge(census_codes[['Region', 'LocationSystem']],
                  left_on=['Region'], right_on=['Region'], how='left')
    # replace Location value
    df['LocationSystem_x'] = np.where(~df['LocationSystem_y'].isnull(),
                                      df['LocationSystem_y'], df['LocationSystem_x'])

    # drop census columns
    df = df.drop(columns=['Name', 'Region', 'LocationSystem_y'])
    df = df.rename(columns={'LocationSystem_x': 'LocationSystem'})

    return df


def call_country_code(country):
    """
    use pycountry to call on 3 digit iso country code, unless pycountry is
    unavailable. As of 10/20/2021, the only country this was called for was
    Canada.
    :param country: str, name of country
    :return: str, ISO code
    """
    if pycountry_available:
        country_info = pycountry.countries.get(name=country)
        country_numeric_iso = country_info.numeric
    elif country.strlower() == 'canada':
        country_numeric_iso = '124'
    else:
        raise ValueError('Country name unknown. pycountry not available to look up.')
    return country_numeric_iso


def convert_fba_unit(df):
    """
    Convert unit to standard
    :param df: df, FBA flowbyactivity
    :return: df, FBA with standarized units
    """
    # Convert Water units 'Bgal/d' and 'Mgal/d' to Mgal
    days_in_year = 365
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Bgal/d',
                                       df['FlowAmount'] * 1000 * days_in_year, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Bgal/d', 'Mgal', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Mgal/d',
                                       df['FlowAmount'] * days_in_year, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal/d', 'Mgal', df['Unit'])

    # Convert Land unit 'Thousand Acres' to 'Acres
    acres_in_thousand_acres = 1000
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Thousand Acres',
                                       df['FlowAmount'] * acres_in_thousand_acres,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Thousand Acres', 'Acres', df['Unit'])

    # Convert Energy unit "Quadrillion Btu" to MJ
    mj_in_btu = .0010550559
    # 1 Quad = .0010550559 x 10^15
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Quadrillion Btu',
                                       df['FlowAmount'] * mj_in_btu * (10 ** 15),
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Quadrillion Btu', 'MJ', df['Unit'])

    # Convert Energy unit "Trillion Btu" to MJ
    # 1 Tril = .0010550559 x 10^14
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Trillion Btu',
                                       df['FlowAmount'] * mj_in_btu * (10 ** 14),
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Trillion Btu', 'MJ', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'million Cubic metres/year',
                                       df['FlowAmount'] * 264.172, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'million Cubic metres/year', 'Mgal', df['Unit'])

    # Convert mass units (LB or TON) to kg
    ton_to_kg = 907.185
    lb_to_kg = 0.45359
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'TON',
                                       df['FlowAmount'] * ton_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'LB',
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
    while (load_yaml_dict('source_catalog').get(sourcename) is None) & (
            '_' in sourcename):
        sourcename = sourcename.rsplit("_", 1)[0]
    return sourcename


def check_activities_sector_like(df_load, sourcename=None):
    """
    Check if the activities in a df are sector-like,
    if cannot find the sourcename in the source catalog, drop extensions on the
    source name
    :param df_load: df, df to determine if activities are sector-like
    :param source: str, optionial, can identify sourcename to use
    """
    # identify sourcename
    if sourcename is not None:
        s = sourcename
    else:
        if 'SourceName' in df_load.columns:
            s = pd.unique(df_load['SourceName'])[0]
        elif 'MetaSources' in df_load.columns:
            s = pd.unique(df_load['MetaSources'])[0]

    sourcename = return_true_source_catalog_name(s)

    try:
        sectorLike = load_yaml_dict('source_catalog')[sourcename][
            'sector-like_activities']
    except KeyError:
        log.info(f'%s not found in {datapath}source_catalog.yaml, assuming '
                 f'activities are not sector-like', sourcename)
        sectorLike = False

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
