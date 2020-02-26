# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Add docstring in public module."""  # TODO add docstring.

import sys
import os
import pandas as pd
import logging as log


log.basicConfig(level=log.INFO, format='%(levelname)s %(message)s',
                stream=sys.stdout)
try:
    modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    modulepath = 'flowsa/'

datapath = modulepath + 'data/'
sourceconfigpath = datapath + 'sourceconfig/'
outputpath = modulepath + 'output/'

US_FIPS = "00000"

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
                    'Compartment': [{'dtype': 'str'}, {'required': True}],
                    'FIPS': [{'dtype': 'str'}, {'required': True}],
                    'Year': [{'dtype': 'int'}, {'required': True}],
                    'DataReliability': [{'dtype': 'float'}, {'required': True}],
                    'DataCollection': [{'dtype': 'float'}, {'required': True}], 
                    'Description': [{'dtype': 'float'}, {'required': True}]
                    }

def getFIPS(state=None, county=None):
    """
    Pass a state or state and county name to get the FIPS.

    :param state: str. A US State Name or Puerto Rico, any case accepted
    :param county: str.
    :return: str. A five digit 2017 FIPS code
    """
    FIPS_df = pd.read_csv(datapath+"FIPS.csv", header=0, dtype={"FIPS": str})

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
