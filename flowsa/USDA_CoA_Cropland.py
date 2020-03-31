# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
import io
import pandas as pd
import json
from flowsa.datapull import * #import load_sourceconfig, store_flowbyactivity, make_http_request, load_api_key #, get_year_from_url
from flowsa.common import log, flow_by_activity_fields, withdrawn_keyword, US_FIPS

source = 'USDA_CoA_Cropland'

def CoA_Cropland_URL_helper(build_url):
    urls_cropland = []
    FIPS_2 = get_all_state_FIPS_2()['State']
    for c in FIPS_2:
        url = build_url
        url = url.replace("__stateAlpha__", c)
        urls_cropland.append(url)
    return urls_cropland




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

def generate_url(url_order, create_url, type_urls):
    if url_final_list == []:
            if type(create_url) is str:
                if url_order == "key":
                    key_value = load_api_key(source)
                    create_url = + create_url + "=" + key_value
                url_final_list.append(format_url_values(create_url))
            #YAMLS currently do not have a dictonary or list as the start of the URL. 
            # Due to time conciderations this has not been tested.
            # elif type(create_url) is list:
            #     for u in create_url:
            #         url_final_list.append(u)
            # elif type(create_url) is dict:
            #     dict_list = generateDict(create_url)
            #     for d in dict_list:
            #         url_final_list.append(d)
    else:
        if type(create_url) is list:
            for i in range(len(url_final_list)):
                url_previous = url_final_list[i]
                for j in range(len(create_url)):
                    url = url_previous + format_url_values(create_url[j]) + "/"
                    if j == 0:
                        url_final_list[i] = url
                    else:
                        url_final_list.append(url)
        elif type(create_url) is str:
            for i in range(len(url_final_list)):
                if url_order == "key":
                    key_value = load_api_key(source)
                    create_url = create_url + "=" + key_value
                url = url_final_list[i]
                url = url + format_url_values(create_url)
                url_final_list[i] = url
        elif type(create_url) is dict:
            dict_list = generate_dict(create_url, url_order)

            for i in range(len(url_final_list)):
                url_previous = url_final_list[i]
                for j in range(len(dict_list)):
                    if url_order == "url_params":
                        url = url_previous + dict_list[j]
                    else:
                        url = url_previous + dict_list[j] + "/"
                    
                    if j == 0:
                        url_final_list[i] = url
                    else:
                        url_final_list.append(url)
    return url_final_list


def generate_dict(dict_values, url_order):
    dictonary_list = []
    for k in dict_values:
        if dictonary_list == []:
            if type(dict_values[k]) is list:
                if url_order == "url_params":
                    for u in dict_values[k]:
                        dictonary_list.append("&" + k + "=" + format_url_values(dict_values[k]))
                else:
                    for u in dict_values[k]:
                        dictonary_list.append(format_url_values(dict_values[k]) + "/")
            elif type(dict_values[k]) is str:
                 if url_order == "url_params":
                    dictonary_list.append("&" + k + "=" + format_url_values(dict_values[k]))
                 else:
                    dictonary_list.append(format_url_values(dict_values[k]) + "/")
 
            #YAMLS currently do not have a dictonary of dictionaries but if we do it will involve recursion
            #This is the start of this method. Due to time conciderations this has not been tested.
            # elif type(d) is dict:
            #     dict_list = generateDict(create_url)
            #     for d in dict_list:
            #         dictonary_list.append(d)
        else:
            if type(dict_values[k]) is list:
                for i in range(len(dictonary_list)):
                    previous_dictonary_list = dictonary_list[i]
                    
                    if url_order == "url_params":
                        values_list = dict_values[k]
                        for j in range(len(dict_values[k])):
                            url = previous_dictonary_list + "&" + k + "=" + format_url_values(values_list[j])
                            if j == 0:
                                dictonary_list[i] = url
                            else:
                                dictonary_list.append(url)
                    else:

                        for j in range(len(dict_values[k])):
                            url = previous_dictonary_list + format_url_values(values_list[j]) + "/"
                            if j == 0:
                                dictonary_list[i] = url
                            else:
                                dictonary_list.append(url)                        
            elif type(dict_values[k]) is str:
                for i in range(len(dictonary_list)):
                    if url_order == "url_params":
                        previous_dictonary_list = dictonary_list[i]
                        dictonary_list[i] = previous_dictonary_list + "&" + k + "=" + dict_values[k]
                    else:
                        previous_dictonary_list = dictonary_list[i]
                        dictonary_list[i] = previous_dictonary_list + dict_values[k] + "/"
        #YAMLS currently do not have a dictonary of dictionaries but if we do it will involve recursion
        #This is the start of this method. Due to time conciderations this has not been tested.
            # elif type(d) is dict:
            #     dict_list = generateDict(d)
            #     for i in range(len(dictonary_list)):
            #         url = dictonary_list[i]
            #         for j in range(len(dict_list)):
            #             url = url + dict_list[i]
            #             if j == 0:
            #                 dictonary_list[i] = url
            #             else:
            #                 dictonary_list.append(url)
    return dictonary_list


def build_url_list(config, source):
    """
    
    :param config: 
    :return: list of urls
    """
    for k,v in config.items():
        if k == "url":
            url_list = []
            url_order = []
            for g in v:
                if g == "county":
                    geo = v[g]
                    for r in geo:
                        if r == "url_order":
                            url_order = geo[r]
                            create_url = []
                            type_urls =[]
                            for p in url_order:
                                create_url.append(geo[p])
                                type_urls.append(check_url_type(geo[p]))
                               
            for i in range(len(url_order)):
            #for d in url_order:
                print(url_order[i], create_url[i], type_urls[i])
                url_list = generate_url(url_order[i], create_url[i], type_urls[i])
                for d in url_final_list:
                    print(d)
    return url_list

def build_usda_crop_url_list(config):
    """
    
    :param config: 
    :return: list of urls
    """
    for k,v in config.items():
        key = load_api_key("USDA_Quickstats")
        if (k == "url_usda_crops"):
            url_list = []
            years = v["year"]
            groups = v["group_desc"]
            agg_level_descs = v["agg_level_desc"]
            base_url = v["base_url"]
            url_key = "key=" + str(key)
            param_source_desc = "&source_desc=" + str(v["source_desc"])
            param_sector_desc = "&sector_desc=" + str(v["sector_desc"])
            param_group_desc = "&group_desc="
            

            for a in agg_level_descs:
              for y in years:
                param_agg_level_desc = "&agg_level_desc=" + str(a)
                param_year = "&year=" + str(y)
                if a == "NATIONAL":
                    for g in groups: 
                        g = format_url_values(g)
                        url = "{0}{1}{2}{3}{4}{5}{6}{7}".format(base_url, url_key, param_source_desc,
                                                                param_sector_desc, param_group_desc,g,
                                                                param_agg_level_desc,param_year)
                        url_list.append(url)
                else:
                    for s in v["state_alpha"]:
                        param_state_alpha = "&state_alpha=" + str(s)
                        for g in groups:
                            g = format_url_values(g)
                            url = "{0}{1}{2}{3}{4}{5}{6}{7}{8}".format(base_url, url_key, param_source_desc,
                                                            param_sector_desc, param_group_desc,g,
                                                            param_agg_level_desc, param_state_alpha, param_year) 
                            url_list.append(url)
    return url_list


def call_usda_crop_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frame_dictionary = {}
    for url in url_list:
        year = get_year_from_url(url)
        r = make_http_request(url)
        df = parse_data(r.text)
        if not data_frame_dictionary.get(year, None):
            data_frame_dictionary[year] = []
        data_frame_dictionary[year].append(df)

    return data_frame_dictionary

def check_value(value):
    if "(D)" in value:
        value = withdrawn_keyword
    elif "," in value:
        if "." in value:
            value = value 
        comma_split = value.split(",")
        value_str = ""
        for c in comma_split:
            value_str = value_str + c
        value = value_str
    elif "." in value:
        value = value 
    return value

def parse_data(text):
    json1_data = json.loads(text)
    data = json1_data["data"]
    class_list = []
    source_name_list = []
    flow_name_list = []
    flow_amount_list = []
    unit_list = []
    activity_produced_list = []
    activity_consumed_list = []
    compartment_list = []
    fips_list = []
    year_list = []
    data_reliability_list = []
    data_collection_list = []
    description_list = []
    measure_of_spread_list = []
    spread_list = []
    distribution_type_list = []
    min_list = []
    max_list = []

    for d in data:
            
            stat_desc = ["AREA HARVESTED", "AREA IN PRODUCTION"]
            do_desc = d["domain_desc"] 
            if any(s in d["statisticcat_desc"] for s in stat_desc):
                if (d["statisticcat_desc"] == "AREA HARVESTED" and (do_desc == "AREA HARVESTED" or do_desc == "TOTAL" or do_desc == "NAICS CLASSIFICATIONS")) or (d["statisticcat_desc"] == "AREA IN PRODUCTION" and (do_desc == "AREA IN PRODUCTION" or do_desc == "TOTAL")):
                    if("fresh market" not in d["short_desc"].lower() and "processing" not in d["short_desc"].lower() and "irrigated, entire crop" not in d["short_desc"].lower() and "irrigated, none of crop" not in d["short_desc"].lower() and "irrigated, part of crop" not in d["short_desc"].lower()):
                        class_list.append("Land")
                        source_name_list.append(source)
                        
                        flow_amount_list.append(check_value(d["Value"]))
                        if d["unit_desc"] == "OPERATIONS":
                            unit_list.append("p")
                            flow_name_list.append(str(d["unit_desc"]))
                            activity_consumed_list.append(None)   
                            if "-" in d["short_desc"] and "sunflower, non-oil type" not in d["short_desc"].lower():
                                description = d["short_desc"].split("-")
                                activity = description[0].strip()
                                if "IRRIGATED" in activity:
                                    activities = activity.split(", IRRIGATED")
                                    activity_produced_list.append(activities[0] + activities[1])
                                else:
                                    activity_produced_list.append(activity)
                            else:
                                activity_produced_list.append(str(d["commodity_desc"]))
                        else:
                            unit_list.append(d["unit_desc"])
                            flow_name_list.append(str(d["statisticcat_desc"]))
                            activity_produced_list.append(None)   
                            if "-" in d["short_desc"] and "sunflower, non-oil type" not in d["short_desc"].lower():
                                description = d["short_desc"].split("-")
                                activity = description[0].strip()
                                if "IRRIGATED" in activity:
                                    activities = activity.split(", IRRIGATED")
                                    activity_consumed_list.append(activities[0] + activities[1])
                                else:
                                    activity_consumed_list.append(activity)
                            else:
                                activity_consumed_list.append(str(d["commodity_desc"]))
                           
                        spread = check_value(d["CV (%)"])
                        spread_list.append(spread)
                        if spread == "W":
                            measure_of_spread_list.append(None)
                        else:
                            measure_of_spread_list.append("RSD")
                        distribution_type_list.append(None)
                        min_list.append(None)
                        max_list.append(None)
                        
                        if d["agg_level_desc"] == "NATIONAL":
                            fips_list.append(US_FIPS)
                        else:
                            if d["county_code"] == "":
                                fips_list.append(str(d["state_fips_code"])+str("000"))
                            else:
                                fips_list.append(str(d["state_fips_code"])+str(d["county_code"]))
                        year_list.append(int(d["year"]))
                        data_reliability_list.append(None)
                        data_collection_list.append(2)
                        compartment = ""
                        if("IRRIGATED" in d["short_desc"]):
                            compartment = "IRRIGATED " 
                        
                        if "(" in d["domaincat_desc"]:
                            domaincat_desc_split = d["domaincat_desc"].split("(")
                            domaincat_desc = "(" + domaincat_desc_split[1]
                            compartment_list.append(compartment + domaincat_desc)
                            description_list.append(str(d["short_desc"]) +" "+ str(d["domain_desc"]) +" "+ domaincat_desc)
                        else:
                            description_list.append(str(d["short_desc"]) +" "+ str(d["domain_desc"]) +" "+ str(d["domaincat_desc"]))
                            if compartment == "":
                                compartment_list.append(None)
                            else:
                                compartment_list.append(compartment.strip())
                
    flow_by_activity = []
    for key in flow_by_activity_fields.keys():
        flow_by_activity.append(key)
    dict = {flow_by_activity[0]: class_list, 
            flow_by_activity[1]: source_name_list,
            flow_by_activity[2]: flow_name_list, 
            flow_by_activity[3]: flow_amount_list,
            flow_by_activity[4]: unit_list,
            flow_by_activity[5]: activity_produced_list,
            flow_by_activity[6]: activity_consumed_list,
            flow_by_activity[7]: compartment_list, 
            flow_by_activity[8]: fips_list,
            flow_by_activity[9]: year_list, 
            flow_by_activity[10]: measure_of_spread_list,
            flow_by_activity[11]: spread_list,
            flow_by_activity[12]: distribution_type_list,
            flow_by_activity[13]: min_list,
            flow_by_activity[14]: max_list,
            flow_by_activity[15]: data_reliability_list,
            flow_by_activity[16]: data_collection_list,
            flow_by_activity[17]: description_list}
    df = pd.DataFrame(dict)
    return df



# if __name__ == '__main__':
#     config = load_sourceconfig(source)
#     url_list = build_url_list(config, source)
#     df_lists = call_usda_crop_urls(url_list[0:52])
#
#     for d in df_lists:
#         df = pd.concat(df_lists[d])
#         log.info("Retrieved data for " + source + " " + d)
#         store_flowbyactivity(df, source, d)
