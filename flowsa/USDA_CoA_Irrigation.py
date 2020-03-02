import io
import pandas as pd
import json
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request
from flowsa.common import log, flow_by_activity_fields, withdrawn_keyword

source = 'USDA_CoA_Irrigation'
def build_usda_crop_url_list(config):
    """
    
    :param config: 
    :return: list of urls
    """
    for k,v in config.items():
        if (k == "url_usda_crops"):
            url_list = []
            years = v["year"]
            agg_level_descs = v["agg_level_desc"]
            base_url = v["base_url"]
            url_key = "key=" + v["url_key"]
            param_source_desc = "&source_desc=" + str(v["source_desc"])
            param_sector_desc = "&sector_desc=" + str(v["sector_desc"])
            param_group_desc = "&group_desc=" + str(v["group_desc"])
            

            for a in agg_level_descs:
              for y in years:
                param_agg_level_desc = "&agg_level_desc=" + str(a)
                param_year = "&year=" + str(y)
                if a == "NATIONAL":
                    url = "{0}{1}{2}{3}{4}{5}{6}".format(base_url, url_key, param_source_desc,
                                                            param_sector_desc, param_group_desc,
                                                            param_agg_level_desc,param_year)
                    url_list.append(url)
                else:
                    for s in v["state_alpha"]:
                        param_state_alpha = "&state_alpha=" + str(s)
                        url = "{0}{1}{2}{3}{4}{5}{6}{7}".format(base_url, url_key, param_source_desc,
                                                            param_sector_desc, param_group_desc,
                                                            param_agg_level_desc, param_state_alpha, param_year) 
                        url_list.append(url)
    return url_list

def call_usda_crop_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in url_list:
        r = make_http_request(url)
        df = parse_data(r.text)
        data_frames_list.append(df)
    return data_frames_list


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


    for d in data:
        if "CROPS" in d["sector_desc"]:
            if "IRRIGATED - ACRES IN PRODUCTION" in d["short_desc"]:
                class_list.append("water")
                source_name_list.append(source)
                flow_name_list.append("Irrigated -" + d["prodn_practice_desc"])
                if "(D)" in d["Value"]:
                    flow_amount_list.append(withdrawn_keyword)
                else:
                    flow_amount_list.append(d["Value"])
                unit_list.append(d["unit_desc"])
                activity_produced_list.append(None)
                activity_consumed_list.append( "the crop, " + str(d["commodity_desc"]))
                compartment_list.append(None)
                if d["county_code"] == "":
                    fips_list.append(d["state_fips_code"]+"000")
                else:
                    fips_list.append(str(d["state_fips_code"])+str(d["county_code"]))
                year_list.append(d["year"])
                data_reliability_list.append(None)
                data_collection_list.append(None)
                description_list.append(d["short_desc"])

    flow_by_activity = []
    for key in flow_by_activity_fields.keys():
        flow_by_activity.append(key)
    dict = {flow_by_activity[0]: class_list, flow_by_activity[1]: source_name_list,
            flow_by_activity[2]: flow_name_list, flow_by_activity[3]: flow_amount_list,
            flow_by_activity[4]: unit_list,
            flow_by_activity[5]: activity_produced_list,
            flow_by_activity[6]: activity_consumed_list,
            flow_by_activity[7]: compartment_list, flow_by_activity[8]: fips_list,
            flow_by_activity[9]: year_list, flow_by_activity[10]: data_reliability_list,
            flow_by_activity[11]: data_collection_list,
            flow_by_activity[12]: description_list}
    df = pd.DataFrame(dict)
    return df
if __name__ == '__main__':
    config = load_sourceconfig(source)
    url_list = build_usda_crop_url_list(config)
    df_list = call_usda_crop_urls(url_list)
    df = pd.concat(df_list)
    log.info("Retrieved data for " + source)
    store_flowbyactivity(df, source)
