# datapull.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Methods for pulling data from http sources
File configuration requires a year for the data pull and a data source (yaml file name) as parameters
"""
import pandas as pd
import argparse
import yaml
import requests
import json
import sys # used to replace keywords in urls

from flowsa.common import outputpath, sourceconfigpath, log, local_storage_path, \
    flow_by_activity_fields, get_all_state_FIPS_2

from flowsa.USDA_CoA_Cropland import *
#from flowsa.USGS_Water_Use import *
#from flowsa.BLS_QCEW import *
from flowsa.Census_CBP import *


def parse_args():
    # Make year and source script parameters
    ap = argparse.ArgumentParser()
    ap.add_argument("-y", "--year", required=True, help="Year for data pull and save")
    ap.add_argument("-s", "--source", required=True, help="Data source code to pull and save")
    args = vars(ap.parse_args())
    return args


url_final_list = []


def store_flowbyactivity(result, source, year=None):
    """Prints the data frame into a parquet file."""
    if year is not None:
        f = outputpath + source + "_" + str(year) + '.parquet'
    else:
        f = outputpath + source + '.parquet'
    try:
        result.to_parquet(f,engine="pyarrow")
    except:
        log.error('Failed to save '+source + "_" + str(year) +' file.')

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

def load_sourceconfig(source):
    sfile = sourceconfigpath+source+'.yaml'
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config

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
    keyfile = local_storage_path+'/'+ api_source + '_API_KEY.txt'
    key = ""
    try:
        with open(keyfile,mode='r') as keyfilecontents:
            key = keyfilecontents.read()
    except IOError:
        log.error("Key file not found.")
    return key


def load_json_from_requests_response(response_w_json):
    response_json = json.loads(response_w_json.text)
    return response_json


def format_url_values(string):
    if " " in string:
        string = string.replace(" ", "%20")
    if "&" in string:
        string = string.replace("&", "%26")
    return string


def check_url_type(url_obj):
    if type(url_obj) is list:
        return "list"
    elif type(url_obj) is str:
        return "string"
    elif type(url_obj) is dict:
        return "dict"
    else:
        return "None of the above"


def build_url_for_query(urlinfo):
    """Creates a base url which requires string substitutions that depend on data source"""
    params = ""
    for k,v in urlinfo['url_params'].items():
        params = params+'&'+k+"="+str(v)

    build_url = "{0}{1}{2}".format(urlinfo['base_url'], urlinfo['api_path'], params)

    # substitute year from arguments and users api key into the url
    if "__year__" in build_url:
        build_url = build_url.replace("__year__", args["year"])
    if "__apiKey__" in build_url:
        userAPIKey = load_api_key(config['api_name'])
        build_url = build_url.replace("__apiKey__", userAPIKey)
    return build_url


def assemble_urls_for_query(build_url):
    """Calls on helper functions defined in source.py files to replace parts of the url string"""
    if hasattr(sys.modules[__name__], config["url_replace_fxn"]):
        urls = getattr(sys.modules[__name__], config["url_replace_fxn"])(build_url)
        return urls
    # else:
    #    urls = url
    #    return urls

def call_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in url_list:
        log.info("Calling "+ url)
        r = make_http_request(url)
        jsonLoad = load_json_from_requests_response(r)
        # Convert response
        df = pd.DataFrame(data=jsonLoad[1:len(jsonLoad)], columns=jsonLoad[0])
        data_frames_list.append(df)
    return data_frames_list




if __name__ == '__main__':
    args = parse_args()
    config = load_sourceconfig(args['source'])
    build_url = build_url_for_query(config['url'])
    urls = assemble_urls_for_query(build_url)
    df_list = call_urls(urls)
    print(args)
    print(config)
    print(build_url)
    print(urls)
    print(df_list)
    # df_list = call_cbp_urls(urls)
    # df = pd.concat(df_list)
    #url_list = build_url_list(config, args['source'])






# year is set in parameters, not pulled from url
# def get_year_from_url(url):
#     if "year=" in url:
#         year_split = url.split("year=")
#         year_only = year_split[1].split("&")
#         return year_only[0]
#     else:
#         return None


#    df_lists = call_urls(url_list)
#    df = pd.concat(df_lists[d])
#    log.info("Retrieved data for " + source + " " + d)
#    df = reshape_to_flowbyactivity(df)
#    df = store_metadata(config)
#    store_flowbyactivity(df, source, d)


# def generate_url(url_order, create_url, type_urls, source):
#     if url_final_list == []:
#         if type(create_url) is str:
#             if url_order == "key":
#                 key_value = load_api_key(source)
#                 create_url = + create_url + "=" + key_value
#             url_final_list.append(format_url_values(create_url))
#         # YAMLS currently do not have a dictonary or list as the start of the URL.
#         # Due to time considerations this has not been tested.
#         elif type(create_url) is list:
#             for u in create_url:
#                 url_final_list.append(u)
#         elif type(create_url) is dict:
#             dict_list = generateDict(create_url)
#             for d in dict_list:
#                 url_final_list.append(d)
#     else:
#         if type(create_url) is list:
#             for i in range(len(url_final_list)):
#                 url_previous = url_final_list[i]
#                 for j in range(len(create_url)):
#                     url = url_previous + format_url_values(create_url[j]) + "/"
#                     if j == 0:
#                         url_final_list[i] = url
#                     else:
#                         url_final_list.append(url)
#         elif type(create_url) is str:
#             for i in range(len(url_final_list)):
#                 if url_order == "key":
#                     key_value = load_api_key(source)
#                     create_url = create_url + "=" + key_value
#                 url = url_final_list[i]
#                 url = url + format_url_values(create_url)
#                 url_final_list[i] = url
#         elif type(create_url) is dict:
#             dict_list = generate_dict(create_url, url_order)
#
#             for i in range(len(url_final_list)):
#                 url_previous = url_final_list[i]
#                 for j in range(len(dict_list)):
#                     if url_order == "url_params":
#                         url = url_previous + dict_list[j]
#                     else:
#                         url = url_previous + dict_list[j] + "/"
#
#                     if j == 0:
#                         url_final_list[i] = url
#                     else:
#                         url_final_list.append(url)
#     return url_final_list




# def generate_dict(dict_values, url_order):
#     dictonary_list = []
#     for k in dict_values:
#         if dictonary_list == []:
#             if type(dict_values[k]) is list:
#                 if url_order == "url_params":
#                     for u in dict_values[k]:
#                         dictonary_list.append("&" + k + "=" + format_url_values(dict_values[k]))
#                 else:
#                     for u in dict_values[k]:
#                         dictonary_list.append(format_url_values(dict_values[k]) + "/")
#             elif type(dict_values[k]) is str:
#                 if url_order == "url_params":
#                     dictonary_list.append("&" + k + "=" + format_url_values(dict_values[k]))
#                 else:
#                     dictonary_list.append(format_url_values(dict_values[k]) + "/")
#
#             # YAMLS currently do not have a dictonary of dictionaries but if we do it will involve recursion
#             # This is the start of this method. Due to time conciderations this has not been tested.
#             # elif type(d) is dict:
#             #     dict_list = generateDict(create_url)
#             #     for d in dict_list:
#             #         dictonary_list.append(d)
#         else:
#             if type(dict_values[k]) is list:
#                 for i in range(len(dictonary_list)):
#                     previous_dictonary_list = dictonary_list[i]
#
#                     if url_order == "url_params":
#                         values_list = dict_values[k]
#                         for j in range(len(dict_values[k])):
#                             url = previous_dictonary_list + "&" + k + "=" + format_url_values(values_list[j])
#                             if j == 0:
#                                 dictonary_list[i] = url
#                             else:
#                                 dictonary_list.append(url)
#                     else:
#
#                         for j in range(len(dict_values[k])):
#                             url = previous_dictonary_list + format_url_values(values_list[j]) + "/"
#                             if j == 0:
#                                 dictonary_list[i] = url
#                             else:
#                                 dictonary_list.append(url)
#             elif type(dict_values[k]) is str:
#                 for i in range(len(dictonary_list)):
#                     if url_order == "url_params":
#                         previous_dictonary_list = dictonary_list[i]
#                         dictonary_list[i] = previous_dictonary_list + "&" + k + "=" + dict_values[k]
#                     else:
#                         previous_dictonary_list = dictonary_list[i]
#                         dictonary_list[i] = previous_dictonary_list + dict_values[k] + "/"
#         # YAMLS currently do not have a dictonary of dictionaries but if we do it will involve recursion
#         # This is the start of this method. Due to time conciderations this has not been tested.
#         # elif type(d) is dict:
#         #     dict_list = generateDict(d)
#         #     for i in range(len(dictonary_list)):
#         #         url = dictonary_list[i]
#         #         for j in range(len(dict_list)):
#         #             url = url + dict_list[i]
#         #             if j == 0:
#         #                 dictonary_list[i] = url
#         #             else:
#         #                 dictonary_list.append(url)
#     return dictonary_list







# def build_url_list(config, source):
#     """
#
#     :param config:
#     :return: list of urls
#     """
#     for k, v in config.items():
#         api_source = ""
#         if k == "api_name":
#             api_source = v
#         else:
#             api_source = source
#         if k == "url":
#             url_list = []
#             url_order = []
#             for g in v:
#                 if g == "county":
#                     geo = v[g]
#                     for r in geo:
#                         if r == "url_order":
#                             url_order = geo[r]
#                             create_url = []
#                             type_urls = []
#                             for p in url_order:
#                                 create_url.append(geo[p])
#                                 type_urls.append(check_url_type(geo[p]))
#                 elif g == "state":
#                     geo = v[g]
#                     for r in geo:
#                         if r == "url_order":
#                             url_order = geo[r]
#                             create_url = []
#                             type_urls = []
#                             for p in url_order:
#                                 create_url.append(geo[p])
#                                 type_urls.append(check_url_type(geo[p]))
#                 elif g == "national":
#                     geo = v[g]
#                     for r in geo:
#                         if r == "url_order":
#                             url_order = geo[r]
#                             create_url = []
#                             type_urls = []
#                             for p in url_order:
#                                 create_url.append(geo[p])
#                                 type_urls.append(check_url_type(geo[p]))
#
#             for i in range(len(url_order)):
#                 url_list = generate_url(url_order[i], create_url[i], type_urls[i], api_source)
#     return url_list