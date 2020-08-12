# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Common variables and functions used across flowsa"""

import sys
import os
import yaml
import requests
import pandas as pd
import numpy as np
import logging as log
import appdirs

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

sourceconfigpath = datapath + 'sourceconfig/'
flowbyactivitymethodpath = datapath + 'flowbysectormethods/'
fbaoutputpath = outputpath + 'FlowByActivity/'
fbsoutputpath = outputpath + 'FlowBySector/'

local_storage_path = appdirs.user_data_dir()

US_FIPS = "00000"
fips_number_key = {"national": 0,
                   "state": 2,
                   "county":5}

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
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    return r

def load_sector_crosswalk():
    cw = pd.read_csv(datapath + "NAICS_07_to_17_Crosswalk.csv", dtype="str")
    return cw

def load_sector_length_crosswalk():
    cw = pd.read_csv(datapath + 'NAICS_2012_Crosswalk.csv', dtype='str')
    return cw

def load_household_sector_codes():
    household = pd.read_csv(datapath + 'Household_SectorCodes.csv', dtype='str')
    return household

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
                         'GeographicCorrelation': [{'dtype': 'float'}, {'required': True}],
                         'TechnologicalCorrelation': [{'dtype': 'float'}, {'required': True}],
                         'DataCollection': [{'dtype': 'float'}, {'required': True}]
                         }

# A list of activity fields in each flow data format
activity_fields = {'ProducedBy': [{'flowbyactivity':'ActivityProducedBy'},
                                  {'flowbysector': 'SectorProducedBy'}],
                   'ConsumedBy': [{'flowbyactivity':'ActivityConsumedBy'},
                                  {'flowbysector': 'SectorConsumedBy'}]
                   }


def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names, return df with one column """
    # create single df representing all selected years
    df = []
    for y in years:
        df = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df.append(df)

    column_activities = df[["ActivityConsumedBy", "ActivityProducedBy"]].values.ravel()
    unique_activities = pd.unique(column_activities)
    df_unique = unique_activities.reshape((-1, 1))
    df_unique = pd.DataFrame({'Activity': df_unique[:, 0]})
    df_unique = df_unique.loc[df_unique['Activity'] != 'None']

    # sort df
    df_unique = df_unique.sort_values(['Activity']).reset_index(drop=True)

    return df_unique


def generalize_activity_field_names(df):
    """
    The 'activityconsumedby' and 'activityproducedby' columns from the allocation dataset do not always align with
    the water use dataframe. Generalize the allocation activity column.
    :param fba_df:
    :return:
    """

    # if an activity field column is all 'none', drop the column and rename renaming activity columns to generalize
    for k, v in activity_fields.items():
        if df[v[0]["flowbyactivity"]].all() == 'None':
            df = df.drop(columns=[v[0]["flowbyactivity"]])
        else:
            df = df.rename(columns={v[0]["flowbyactivity"]: 'Activity'})
        if df[v[1]["flowbysector"]].all() == 'None':
            df = df.drop(columns=[v[1]["flowbysector"]])
        else:
            df = df.rename(columns={v[1]["flowbysector"]: 'Sector'})

    return df


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

    # comment out - going to use fips crosswalk to ensure all possible fips included in list
    # FIPS_df = pd.read_csv(datapath + "FIPS.csv", header=0, dtype={"FIPS": str})

    FIPS_df = pd.read_csv(datapath + "FIPS_Crosswalk.csv", header=0, dtype={"FIPS": str})
    # subset columns by specified year
    df = FIPS_df[["State", "FIPS_" + year, "County_" + year]]
    # rename columns
    cols = ['State', 'FIPS', 'County']
    df.columns = cols
    # ensure that FIPS retain leading 0s
    df.loc[:, 'FIPS'] = df['FIPS'].apply('{:0>5}'.format)
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


def convert_fba_unit(df):
    """
    Convert unit to standard
    :param df: Either flowbyactivity
    :return: Df with standarized units
    """
    # remove temporal aspect of unit and want all flows in Mgal
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Bgal/d', df['FlowAmount'] * 1000 * 365, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Bgal/d', 'Mgal', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Mgal/d', df['FlowAmount'] * 365, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal/d', 'Mgal', df['Unit'])

    return df

