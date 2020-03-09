# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
import io
import pandas as pd
import json
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request, load_api_key, get_year_from_url
from flowsa.common import log, flow_by_activity_fields, withdrawn_keyword, US_FIPS

source = 'USDA_CoA_Cropland'
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
        if "CROPS" in d["sector_desc"]:
          #  if "IRRIGATED - ACRES IN PRODUCTION" in d["short_desc"]:
                class_list.append("Land")
                source_name_list.append(source)
                flow_name_list.append(str(d["statisticcat_desc"]))
                flow_amount_list.append(check_value(d["Value"]))
                unit_list.append(d["unit_desc"])
                activity_produced_list.append(None)
                activity_consumed_list.append(str(d["commodity_desc"]))
                measure_of_spread_list.append(check_value(d["CV (%)"]))
                spread_list.append(None)
                distribution_type_list.append(None)
                min_list.append(None)
                max_list.append(None)
                compartment_list.append(None)
                if d["agg_level_desc"] == "NATIONAL":
                    fips_list.append(US_FIPS)
                else:
                    if d["county_code"] == "":
                        fips_list.append(str(d["state_fips_code"])+str("000"))
                    else:
                        fips_list.append(str(d["state_fips_code"])+str(d["county_code"]))
                year_list.append(d["year"])
                data_reliability_list.append(None)
                data_collection_list.append(None)
                if "(" in d["domaincat_desc"]:
                    domaincat_desc_split = d["domaincat_desc"].split("(")
                    domaincat_desc = "(" + domaincat_desc_split[1]
                    description_list.append(str(d["short_desc"]) + str(d["domain_desc"]) + domaincat_desc)
                else:
                    description_list.append(str(d["short_desc"]) + str(d["domain_desc"]) + str(d["domaincat_desc"]))
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

if __name__ == '__main__':
    config = load_sourceconfig(source)
    url_list = build_usda_crop_url_list(config)
    df_lists = call_usda_crop_urls(url_list[0:52])
    
    for d in df_lists:
        df = pd.concat(df_lists[d])
        log.info("Retrieved data for " + source + " " + d)
        store_flowbyactivity(df, source, d)
