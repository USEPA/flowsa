# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used across flowsa"""

import os
import pprint
from os import path
import re
import yaml
import pandas as pd
import numpy as np
from copy import deepcopy
from dotenv import load_dotenv
import flowsa.flowsa_yaml as flowsa_yaml
import flowsa.exceptions
from flowsa.flowsa_log import log
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields, \
    flow_by_activity_wsec_fields, flow_by_activity_mapped_wsec_fields, \
    activity_fields
from flowsa.settings import datapath, MODULEPATH, \
    sourceconfigpath, flowbysectormethodpath, methodpath


# Sets default Sector Source Name
SECTOR_SOURCE_NAME = 'NAICS_2012_Code'
flow_types = ['ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW', 'WASTE_FLOW']

sector_level_key = {"NAICS_2": 2,
                    "NAICS_3": 3,
                    "NAICS_4": 4,
                    "NAICS_5": 5,
                    "NAICS_6": 6,
                    "NAICS_7": 7}

# withdrawn keyword changed to "none" over "W"
# because unable to run calculation functions with text string
WITHDRAWN_KEYWORD = np.nan


def load_env_file_key(env_file, key):
    """
    Loads an API Key from "API_Keys.env" file using the
    'api_name' defined in the FBA source config file. The '.env' file contains
    the users personal API keys. The user must register with this
    API and get the key and manually add to "API_Keys.env"

    See wiki for how to get an api:
    https://github.com/USEPA/flowsa/wiki/Using-FLOWSA#api-keys

    :param env_file: str, name of env to load, either 'API_Key'
    or 'external_path'
    :param key: str, name of source/key defined in env file, like 'BEA' or
    'Census'
    :return: str, value of the key stored in the env
    """
    if env_file == 'API_Key':
        load_dotenv(f'{MODULEPATH}/API_Keys.env', verbose=True)
        value = os.getenv(key)
        if value is None:
            raise flowsa.exceptions.APIError(api_source=key)
    else:
        load_dotenv(f'{MODULEPATH}/external_paths.env', verbose=True)
        value = os.getenv(key)
        if value is None:
            raise flowsa.exceptions.EnvError(key=key)
    return value


def load_crosswalk(crosswalk_name):
    """
    Used to load the crosswalks:

    'NAICS_2012_Crosswalk', 'Sector_2012_Names', 'Sector_2017_Names','Household_SectorCodes',
    'Government_SectorCodes', 'NAICS_to_BEA_Crosswalk_2012',
    'NAICS_to_BEA_Crosswalk_2017', 'NAICS_Year_Concordance'

    as a dataframe

    :return: df, NAICS crosswalk over the years
    """

    cw = pd.read_csv(datapath / f'{crosswalk_name}.csv', dtype="str")

    return cw


def load_sector_length_cw_melt(year='2012'):
    cw_load = load_crosswalk(f'NAICS_{year}_Crosswalk')
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
    for cw in ['Household_SectorCodes', 'Government_SectorCodes']:
        df = load_crosswalk(cw)
        cw_list.append(df)
    # concat data into single dataframe
    cw = pd.concat(cw_list, sort=False)
    code_list = cw['Code'].drop_duplicates().values.tolist()
    return code_list


def load_yaml_dict(filename, flowbytype=None, filepath=None, **kwargs):
    """
    Load the information in a yaml file, from source_catalog, or FBA,
    or FBS files
    :return: dictionary containing all information in yaml
    """
    # check if the version and githash are included in the filename, if so,
    # drop, but return warning that we might be loading a revised version of
    # the config file. The pattern looks for a "_v" followed by a number
    # between [0-9] followed by a decimal
    pattern = '_v[0-9].*'
    if re.search(pattern, filename):
        log.warning('Filename includes a github version and githash. Dropping '
                 'the version and hash to load most up-to-date yaml config '
                 'file. The yaml config file might not reflect the yaml used '
                 'to generate the dataframe')
        filename = re.sub(pattern,'', filename)

    if filename in ['source_catalog']:
        folder = datapath
    else:
        # first check if a filepath for the yaml is specified, as is the
        # case with FBS method files located outside FLOWSA
        # if filepath is not None:
        if path.exists(path.join(str(filepath), f'{filename}.yaml')):
            log.info(f'Loading {filename} from {filepath}')
            folder = filepath
        elif path.exists(path.join(str(filepath), 'flowbysectormethods/', f'{filename}.yaml')):
            log.info(f'Loading {filename} from {filepath}flowbysectormethods/')
            folder = f'{filepath}flowbysectormethods/'
        else:
            if filepath is not None:
                log.warning(f'{filename} not found in {filepath}. '
                            f'Checking default folders')
            if flowbytype == 'FBA':
                folder = sourceconfigpath
            elif flowbytype == 'FBS':
                folder = flowbysectormethodpath
            else:
                raise KeyError('Must specify either \'FBA\' or \'FBS\'')
    yaml_path = f'{folder}/{filename}.yaml'

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = flowsa_yaml.load(f, filepath)
    except FileNotFoundError:
        if 'config' in kwargs:
            return deepcopy(kwargs['config'])
        else:
            raise flowsa.exceptions.FlowsaMethodNotFoundError(
                method_type=flowbytype, method=filename)
    return config


def load_values_from_literature_citations_config():
    """
    Load the config file that contains information on where the
    values from the literature come from
    :return: dictionary of the values from the literature information
    """
    sfile = (datapath / 'bibliographyinfo' /
             'values_from_literature_source_citations.yaml')
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
        if v[0]['dtype'] in ['str', 'object']:
            fill_na_dict[k] = np.nan
        else:
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


def get_flowsa_base_name(filedirectory, filename, extension):
    """
    If filename does not match filename within flowsa due to added extensions
    onto the filename, cycle through
    name, dropping strings after each underscore until the name is found
    :param filedirectory: Path object, path to directory
    :param filename: string, name of original file searching for
    :param extension: string, type of file, such as "yaml" or "py"
    :return: string, corrected file path name
    """
    # If a file does not exist, modify file name, dropping portion after last
    # underscore. Repeat this process until the file name exists or no
    # underscores are left.
    while '_' in filename:
        if (filedirectory / f"{filename}.{extension}").is_file():
            break
        filename, _ = filename.rsplit('_', 1)

    return filename


def return_true_source_catalog_name(sourcename):
    """
    Drop any extensions on source name until find the name in source catalog
    """
    while (load_yaml_dict('source_catalog').get(sourcename) is None) & (
            '_' in sourcename):
        sourcename = sourcename.rsplit("_", 1)[0]
    return sourcename


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


def check_method_status():
    """Read the current method status"""
    yaml_path = methodpath / 'method_status.yaml'
    with open(yaml_path, 'r') as f:
        method_status = yaml.safe_load(f)
    return method_status


def get_catalog_info(source_name: str) -> dict:
    '''
    Retrieves the information on a given source from source_catalog.yaml.
    Replaces various pieces of code that load the source_catalog yaml.
    '''
    source_catalog = load_yaml_dict('source_catalog')
    source_name = return_true_source_catalog_name(source_name)
    return source_catalog.get(source_name, {})


def seeAvailableFlowByModels(flowbytype, print_method=True):
    """
    Console print and return available Flow-By-Activity or Flow-By-Sector models
    :param flowbytype: 'FBA' or 'FBS'
    :param print_method: False to skip printing to console
    :return: dict or list of available models
    """

    # fb directory contents dependent on FBA or FBS
    if flowbytype == 'FBA':
        fb_dir = os.listdir(sourceconfigpath)
    elif flowbytype == 'FBS':
        fb_dir = os.listdir(flowbysectormethodpath)
    else:
        raise ValueError("flowbytype must be 'FBA' or 'FBS'")

    # list of file names (drop extension) for yaml files in flow directory
    fb_names = [os.path.splitext(f)[0] for f in fb_dir if f.endswith('.yaml')]

    # further reduce list of file names by excluding common and summary_target
    exclude = ["_common", "_Common", "_target"]
    fb_names = [f for f in fb_names if all(s not in f for s in exclude)]

    if flowbytype == 'FBA':
        # create empty dictionary, this will be the data format to print FBA
        data_print = {}
        # iterate over names to build dict for FBA and handling years
        for f in fb_names:
            s = load_yaml_dict(f, 'FBA')
            try:
                years = s['years']
            except KeyError:
                years = 'YAML missing information on years'
            data_print.update({f: years})
    else:
        # data format to print FBS
        data_print = fb_names

    if print_method:
        # print data in human-readable format
        pprint.pprint(data_print, width=79, compact=True)

    return data_print
