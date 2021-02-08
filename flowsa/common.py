# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Common variables and functions used across flowsa"""

import sys
import os
import yaml
import requests
import requests_ftp
import pandas as pd
import numpy as np
import logging as log
import appdirs
import pycountry

log.basicConfig(level=log.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)

try:
    modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    modulepath = 'flowsa/'

# comment out if running test data
datapath = modulepath + 'data/'
outputpath = modulepath + 'output/'

# comment in if running test data
# datapath = (modulepath + 'data/').replace('flowsa/flowsa/', 'flowsa/tests/')
# outputpath = (modulepath + 'output/').replace('flowsa/flowsa/', 'flowsa/tests/')

sourceconfigpath = datapath + 'flowbyactivitymethods/'
crosswalkpath = datapath + 'activitytosectormapping/'
flowbysectormethodpath = datapath + 'flowbysectormethods/'
flowbysectoractivitysetspath = datapath + 'flowbysectoractivitysets/'
externaldatapath = datapath + 'external_data/'
fbaoutputpath = outputpath + 'FlowByActivity/'
fbsoutputpath = outputpath + 'FlowBySector/'

local_storage_path = appdirs.user_data_dir()

US_FIPS = "00000"
fips_number_key = {"national": 0,
                   "state": 2,
                   "county": 5}

sector_level_key = {"NAICS_2": 2,
                    "NAICS_3": 3,
                    "NAICS_4": 4,
                    "NAICS_5": 5,
                    "NAICS_6": 6}

# withdrawn keyword changed to "none" over "W" because unable to run calculation functions with text string
withdrawn_keyword = None

flow_types = ['ELEMENTARY_FLOW','TECHNOSPHERE_FLOW','WASTE_FLOW']

#Sets default Sector Source Name
sector_source_name = 'NAICS_2012_Code'

def load_api_key(api_source):
    """
    Loads a txt file from the appdirs user directory with a set name
    in the form of the host name and '_API_KEY.txt' like 'BEA_API_KEY.txt'
    containing the users personal API key. The user must register with this
    API and get the key and save it to a .txt file in the user directory specified
    by local_storage_path (see common.py for definition)
    :param api_source: str, name of source, like 'BEA' or 'Census'
    :return: the users API key as a string
    """
    keyfile = local_storage_path + '/' + api_source + '_API_KEY.txt'
    key = ""
    try:
        with open(keyfile, mode='r') as keyfilecontents:
            key = keyfilecontents.read()
    except IOError:
        log.error("Key file not found.")
    return key


def make_http_request(url):
    r = []
    try:
        r = requests.get(url)
    except requests.exceptions.InvalidSchema: # if url is ftp rather than http
        requests_ftp.monkeypatch_session()
        r = requests.Session().get(url)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    return r

def load_sector_crosswalk():
    cw = pd.read_csv(datapath + "NAICS_Crosswalk.csv", dtype="str")
    return cw

def load_sector_length_crosswalk():
    cw = pd.read_csv(datapath + 'NAICS_2012_Crosswalk.csv', dtype='str')
    return cw

# def load_sector_length_crosswalk_w_nonnaics():
#     cw = load_sector_length_crosswalk()
#     # append household codes
#     cw = cw.append(pd.DataFrame([["F010", "F010", "F010", "F0100", "F01000"]], columns=cw.columns), ignore_index=True)
#     # append government transportation codes
#     cw = cw.append(pd.DataFrame([["S00201", "S00201", "S00201", "S00201", "S00201"]], columns=cw.columns), ignore_index=True)
#     return cw

def load_household_sector_codes():
    household = pd.read_csv(datapath + 'Household_SectorCodes.csv', dtype='str')
    return household

def load_government_sector_codes():
    government = pd.read_csv(datapath + 'Government_SectorCodes.csv', dtype='str')
    return government

def load_bea_crosswalk():
    cw = pd.read_csv(datapath + "BEA_Crosswalk.csv", dtype="str")
    return cw

def load_source_catalog():
     sources= datapath+'source_catalog.yaml'
     with open(sources, 'r') as f:
         config = yaml.safe_load(f)
     return config

def load_sourceconfig(source):
    sfile = sourceconfigpath+source+'.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config


flow_by_activity_fields = {'Class': [{'dtype': 'str'}, {'required': True}],
                           'SourceName': [{'dtype': 'str'}, {'required': True}],
                           'FlowName': [{'dtype': 'str'}, {'required': True}],
                           'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                           'Unit': [{'dtype': 'str'}, {'required': True}],
                           'FlowType': [{'dtype': 'str'}, {'required': True}],
                           'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
                           'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
                           'Compartment': [{'dtype': 'str'}, {'required': False}],
                           'Location': [{'dtype': 'str'}, {'required': True}],
                           'LocationSystem': [{'dtype': 'str'}, {'required': True}],
                           'Year': [{'dtype': 'int'}, {'required': True}],
                           'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
                           'Spread': [{'dtype': 'float'}, {'required': False}],
                           'DistributionType': [{'dtype': 'str'}, {'required': False}],
                           'Min': [{'dtype': 'float'}, {'required': False}],
                           'Max': [{'dtype': 'float'}, {'required': False}],
                           'DataReliability': [{'dtype': 'float'}, {'required': True}],
                           'DataCollection': [{'dtype': 'float'}, {'required': True}],
                           'Description': [{'dtype': 'str'}, {'required': True}]
                           }

flow_by_sector_fields = {'Flowable': [{'dtype': 'str'}, {'required': True}],
                         'Class': [{'dtype': 'str'}, {'required': True}],
                         'SectorProducedBy': [{'dtype': 'str'}, {'required': False}],
                         'SectorConsumedBy': [{'dtype': 'str'}, {'required': False}],
                         'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
                         'Context': [{'dtype': 'str'}, {'required': True}],
                         'Location': [{'dtype': 'str'}, {'required': True}],
                         'LocationSystem': [{'dtype': 'str'}, {'required': True}],
                         'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                         'Unit': [{'dtype': 'str'}, {'required': True}],
                         'FlowType': [{'dtype': 'str'}, {'required': True}],
                         'Year': [{'dtype': 'int'}, {'required': True}],
                         'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
                         'Spread': [{'dtype': 'float'}, {'required': False}],
                         'DistributionType': [{'dtype': 'str'}, {'required': False}],
                         'Min': [{'dtype': 'float'}, {'required': False}],
                         'Max': [{'dtype': 'float'}, {'required': False}],
                         'DataReliability': [{'dtype': 'float'}, {'required': True}],
                         'TemporalCorrelation': [{'dtype': 'float'}, {'required': True}],
                         'GeographicalCorrelation': [{'dtype': 'float'}, {'required': True}],
                         'TechnologicalCorrelation': [{'dtype': 'float'}, {'required': True}],
                         'DataCollection': [{'dtype': 'float'}, {'required': True}],
                         'MetaSources': [{'dtype': 'str'}, {'required': True}]
                         }

flow_by_sector_fields_w_activity = flow_by_sector_fields.copy()
flow_by_sector_fields_w_activity.update({'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
                                         'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}]})

flow_by_sector_collapsed_fields = {'Flowable': [{'dtype': 'str'}, {'required': True}],
                                   'Class': [{'dtype': 'str'}, {'required': True}],
                                   'Sector': [{'dtype': 'str'}, {'required': False}],
                                   'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
                                   'Context': [{'dtype': 'str'}, {'required': True}],
                                   'Location': [{'dtype': 'str'}, {'required': True}],
                                   'LocationSystem': [{'dtype': 'str'}, {'required': True}],
                                   'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                                   'Unit': [{'dtype': 'str'}, {'required': True}],
                                   'FlowType': [{'dtype': 'str'}, {'required': True}],
                                   'Year': [{'dtype': 'int'}, {'required': True}],
                                   'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
                                   'Spread': [{'dtype': 'float'}, {'required': False}],
                                   'DistributionType': [{'dtype': 'str'}, {'required': False}],
                                   'Min': [{'dtype': 'float'}, {'required': False}],
                                   'Max': [{'dtype': 'float'}, {'required': False}],
                                   'DataReliability': [{'dtype': 'float'}, {'required': True}],
                                   'TemporalCorrelation': [{'dtype': 'float'}, {'required': True}],
                                   'GeographicalCorrelation': [{'dtype': 'float'}, {'required': True}],
                                   'TechnologicalCorrelation': [{'dtype': 'float'}, {'required': True}],
                                   'DataCollection': [{'dtype': 'float'}, {'required': True}],
                                   'MetaSources': [{'dtype': 'str'}, {'required': True}]
                                   }

flow_by_activity_wsec_mapped_fields = {'Class': [{'dtype': 'str'}, {'required': True}],
                                       'SourceName': [{'dtype': 'str'}, {'required': True}],
                                       'FlowName': [{'dtype': 'str'}, {'required': True}],
                                       'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                                       'Unit': [{'dtype': 'str'}, {'required': True}],
                                       'FlowType': [{'dtype': 'str'}, {'required': True}],
                                       'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
                                       'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
                                       'Compartment': [{'dtype': 'str'}, {'required': False}],
                                       'Location': [{'dtype': 'str'}, {'required': True}],
                                       'LocationSystem': [{'dtype': 'str'}, {'required': True}],
                                       'Year': [{'dtype': 'int'}, {'required': True}],
                                       'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
                                       'Spread': [{'dtype': 'float'}, {'required': False}],
                                       'DistributionType': [{'dtype': 'str'}, {'required': False}],
                                       'Min': [{'dtype': 'float'}, {'required': False}],
                                       'Max': [{'dtype': 'float'}, {'required': False}],
                                       'DataReliability': [{'dtype': 'float'}, {'required': True}],
                                       'DataCollection': [{'dtype': 'float'}, {'required': True}],
                                       #'Description': [{'dtype': 'str'}, {'required': True}],
                                       'SectorProducedBy': [{'dtype': 'str'}, {'required': False}],
                                       'SectorConsumedBy': [{'dtype': 'str'}, {'required': False}],
                                       'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
                                       'ProducedBySectorType': [{'dtype': 'str'}, {'required': False}],
                                       'ConsumedBySectorType': [{'dtype': 'str'}, {'required': False}]
                                       }

# A list of activity fields in each flow data format
activity_fields = {'ProducedBy': [{'flowbyactivity':'ActivityProducedBy'},
                                  {'flowbysector': 'SectorProducedBy'}],
                   'ConsumedBy': [{'flowbyactivity':'ActivityConsumedBy'},
                                  {'flowbysector': 'SectorConsumedBy'}]
                   }


def create_fill_na_dict(flow_by_fields):
    fill_na_dict = {}
    for k,v in flow_by_fields.items():
        if v[0]['dtype']=='str':
            fill_na_dict[k] = ""
        elif v[0]['dtype']=='int':
            fill_na_dict[k] = 9999
        elif v[0]['dtype']=='float':
            fill_na_dict[k] = 0.0
    return fill_na_dict


def get_flow_by_groupby_cols(flow_by_fields):
    groupby_cols = []
    for k,v in flow_by_fields.items():
        if v[0]['dtype']=='str':
            groupby_cols.append(k)
        elif v[0]['dtype']=='int':
            groupby_cols.append(k)
    if flow_by_fields == flow_by_activity_fields:
        #Do not use description for grouping
        groupby_cols.remove('Description')
    return groupby_cols


def read_stored_FIPS(year='2015'):
    """
    Read fips based on year specified, year defaults to 2015
    :param year: '2010', '2013', or '2015'
    :return:
    """

    FIPS_df = pd.read_csv(datapath + "FIPS_Crosswalk.csv", header=0, dtype=str)
    # subset columns by specified year
    df = FIPS_df[["State", "FIPS_" + year, "County_" + year]]
    # rename columns
    cols = ['State', 'FIPS', 'County']
    df.columns = cols
    # sort df
    df = df.sort_values(['FIPS']).reset_index(drop=True)

    return df


def getFIPS(state=None, county=None, year='2015'):
    """
    Pass a state or state and county name to get the FIPS.

    :param state: str. A US State Name or Puerto Rico, any case accepted
    :param county: str.
    :param year: str. '2010', '2013', '2015'
    :return: str. A five digit 2017 FIPS code
    """
    FIPS_df = read_stored_FIPS(year)

    if county is None:
        if state is not None:
            state = clean_str_and_capitalize(state)
            code = FIPS_df.loc[(FIPS_df["State"] == state) & (FIPS_df["County"].isna()), "FIPS"]
    else:
        if state is None:
            log.error("To get county FIPS, state name must be passed in 'state' param")
        else:
            state = clean_str_and_capitalize(state)
            county = clean_str_and_capitalize(county)
            code = FIPS_df.loc[(FIPS_df["State"] == state) & (FIPS_df["County"] == county), "FIPS"]
    if code.empty:
        log.info("No FIPS code found")
    else:
        code = code.values[0]
        return code

def apply_county_FIPS(df, year='2015', source_state_abbrev=True):
    """
    Applies FIPS codes by county to dataframe containing columns with State and County
    
    :param df: dataframe must contain columns with 'State' and 'County', but not 'Location'
    :return dataframe with new column 'FIPS', blanks not removed
    """
    # If using 2 letter abbrevations, map to state names
    if source_state_abbrev:
        df['State']=df['State'].map(abbrev_us_state)
    df['State']=df.apply(lambda x: clean_str_and_capitalize(x.State), axis=1)
    df['County']=df.apply(lambda x: clean_str_and_capitalize(x.County), axis=1)
    
    # Pull and merge FIPS on state and county
    mapping_FIPS = get_county_FIPS()
    df = df.merge(mapping_FIPS, how = 'left')
    
    # Where no county match occurs, assign state FIPS instead
    mapping_FIPS = get_state_FIPS()
    mapping_FIPS.drop(columns = ['County'], inplace=True)
    df = df.merge(mapping_FIPS, left_on = 'State', right_on = 'State', how = 'left')
    df['Location']=df['FIPS_x'].where(df['FIPS_x'].notnull(),df['FIPS_y'])
    df.drop(columns = ['FIPS_x','FIPS_y'], inplace=True)
    
    return df

def update_geoscale(df, to_scale):
    """Updates df['Location'] based on specified to_scale"""
    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        df.loc[:, 'Location'] = df['Location'].apply(lambda x: str(x[0:2]))
        # pad zeros
        df.loc[:, 'Location'] = df['Location'].apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5 else x)
    elif to_scale == 'national':
        df.loc[:, 'Location'] = US_FIPS
    return df

def clean_str_and_capitalize(s):
    """Trim whitespace, modify string so first letter capitalized."""
    if s.__class__ == str:
        s = s.strip()
        s = s.lower()
        s = s.capitalize()
    return s


def capitalize_first_letter(string):
    """Capitalize first letter of words"""
    return_string = ""
    split_array = string.split(" ")
    for s in split_array:
        return_string = return_string + " " + s.capitalize()
    return return_string.strip()


def get_state_FIPS(year='2015'):
    """
    Filters FIPS df for state codes only
    :return: FIPS df with only state level records
    """

    fips = read_stored_FIPS(year)
    fips = fips.drop_duplicates(subset='State')
    fips = fips[fips['State'].notnull()]
    return fips


def get_county_FIPS(year='2015'):
    """
    Filters FIPS df for county codes only
    :return: FIPS df with only county level records
    """
    fips = read_stored_FIPS(year)
    fips = fips.drop_duplicates(subset='FIPS')
    fips = fips[fips['County'].notnull()]
    return fips


def get_all_state_FIPS_2(year='2015'):
    """
    Gets a subset of all FIPS 2 digit codes for states
    :return: df with 'State' and 'FIPS_2' cols
    """

    state_fips = get_state_FIPS(year)
    state_fips.loc[:, 'FIPS_2'] = state_fips['FIPS'].apply(lambda x: x[0:2])
    state_fips = state_fips[['State','FIPS_2']]
    return state_fips

#From https://gist.github.com/rogerallen/1583593
#removed non US states, PR, MP, VI
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
    df = pd.read_csv(datapath + "Census_Regions_and_Divisions.csv", dtype="str")
    return df


def assign_census_regions(df_load):

    # load census codes
    census_codes_load = get_region_and_division_codes()
    census_codes = census_codes_load[census_codes_load['LocationSystem'] == 'Census_Region']

    # merge df with census codes
    df = df_load.merge(census_codes[['Name', 'Region']], left_on=['Location'], right_on=['Name'], how='left')
    # replace Location value
    df['Location'] = np.where(~df['Region'].isnull(), df['Region'], df['Location'])

    # modify LocationSystem
    # merge df with census codes
    df = df.merge(census_codes[['Region', 'LocationSystem']], left_on=['Region'], right_on=['Region'], how='left')
    # replace Location value
    df['LocationSystem_x'] = np.where(~df['LocationSystem_y'].isnull(), df['LocationSystem_y'], df['LocationSystem_x'])

    # drop census columns
    df = df.drop(columns=['Name', 'Region', 'LocationSystem_y'])
    df = df.rename(columns={'LocationSystem_x': 'LocationSystem'})

    return df


def call_country_code(country):
    """use pycountry to call on 3 digit iso country code"""
    country_info = pycountry.countries.get(name=country)
    country_numeric_iso = country_info.numeric
    return country_numeric_iso


def convert_fba_unit(df):
    """
    Convert unit to standard
    :param df: Either flowbyactivity
    :return: Df with standarized units
    """
    # Convert Water units 'Bgal/d' and 'Mgal/d' to Mgal
    days_in_year = 365
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Bgal/d', df['FlowAmount'] * 1000 * days_in_year, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Bgal/d', 'Mgal', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Mgal/d', df['FlowAmount'] * days_in_year, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal/d', 'Mgal', df['Unit'])

    # Convert Land unit 'Thousand Acres' to 'Acres
    acres_in_thousand_acres = 1000
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Thousand Acres', df['FlowAmount'] * acres_in_thousand_acres,
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

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'million Cubic metres/year', df['FlowAmount'] * 264.172, df['FlowAmount'])
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

