# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Common variables and functions used across flowsa"""

import sys
import os
import pandas as pd
import logging as log
import appdirs

log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s',
                stream=sys.stdout)
try:
    modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    modulepath = 'flowsa/'

datapath = modulepath + 'data/'
sourceconfigpath = datapath + 'sourceconfig/'
outputpath = modulepath + 'output/'
flowbyactivitymethodpath = datapath +  'flowbysectormethods/'

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

flow_by_activity_fields = {'Class': [{'dtype': 'str'}, {'required': True}],
                           'SourceName': [{'dtype': 'str'}, {'required': True}],
                           'FlowName': [{'dtype': 'str'}, {'required': True}],
                           'FlowAmount': [{'dtype': 'float'}, {'required': True}],
                           'Unit': [{'dtype': 'str'}, {'required': True}],
                           'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
                           'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
                           'Compartment': [{'dtype': 'str'}, {'required': False}],
                           'FIPS': [{'dtype': 'str'}, {'required': True}],
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
                         'FIPS': [{'dtype': 'str'}, {'required': True}],
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

#A list of activity fields in each flow data format
activity_fields = {'ProducedBy': [{'flowbyactivity':'ActivityProducedBy'},
                                  {'flowbysector': 'SectorProducedBy'}],
                   'ConsumedBy': [{'flowbyactivity':'ActivityConsumedBy'},
                                  {'flowbysector': 'SectorConsumedBy'}]
                   }
def read_stored_FIPS():
    FIPS_df = pd.read_csv(datapath + "FIPS.csv", header=0, dtype={"FIPS": str})
    return FIPS_df

def getFIPS(state=None, county=None):
    """
    Pass a state or state and county name to get the FIPS.

    :param state: str. A US State Name or Puerto Rico, any case accepted
    :param county: str.
    :return: str. A five digit 2017 FIPS code
    """
    FIPS_df = read_stored_FIPS()

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
    """Add docstring in public function."""  # TODO add docstring.
    if s.__class__ == str:
        s = s.strip()
        s = s.lower()
        s = s.capitalize()
    return s

def get_state_FIPS():
    """
    Filters FIPS df for state codes only
    :return: FIPS df with only state level records
    """
    fips = read_stored_FIPS()
    fips = fips.drop_duplicates(subset='State')
    fips = fips[fips['State'].notnull()]
    return fips

def get_county_FIPS():
    """
    Filters FIPS df for county codes only
    :return: FIPS df with only county level records
    """
    fips = read_stored_FIPS()
    fips = fips.drop_duplicates(subset='County')
    fips = fips[fips['County'].notnull()]
    return fips



def get_all_state_FIPS_2():
    """
    Gets a subset of all FIPS 2 digit codes for states
    :return: df with 'State' and 'FIPS_2' cols
    """

    state_fips = get_state_FIPS()
    state_fips['FIPS_2'] = state_fips['FIPS'].apply(lambda x: x[0:2])
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

