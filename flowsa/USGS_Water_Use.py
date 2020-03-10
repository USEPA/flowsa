# USGS_Water_Use.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
import io
import pandas as pd
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request, get_year_from_url
from flowsa.common import log, flow_by_activity_fields, withdrawn_keyword, US_FIPS


source = 'USGS_Water_Use'
class_value = 'Water'
technosphere_flow_array = ["consumptive", "Public Supply"]
waste_flow_array = ["wastewater", "loss"]

def build_usgs_water_url_list(config):
    """
    
    :param config: 
    :return: 
    """
    for k, v in config.items():
        if (k == "url"):
            url_list = []
            states = v["states"]
            years = v["wu_year"]
            base_url = v["base_url"]
            url_path = v["url_path"]
            param_format = "format=" + str(v["format"])
            param_compression = "_compression=" + str(v["_compression"])
            param_wu_area = "&wu_area=" + str(v["wu_area"])
            param_wu_year = "&wu_year="
            param_wu_county = "&wu_county=" + str(v["wu_county"])
            param_wu_category = "&wu_category=" + str(v["wu_category"])
            for s in states:
                for y in years:
                    url = "{0}{1}{2}{3}{4}{5}{6}{7}{8}{9}".format(base_url, s, url_path, param_format,
                                                            param_compression, param_wu_area,
                                                            param_wu_year,y,
                                                            param_wu_county, param_wu_category)
                    url_list.append(url)
    return url_list

def call_usgs_water_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frame_dictionary = {}
    for url in url_list:
        year = get_year_from_url(url)
        r = make_http_request(url)
        usgs_split = get_usgs_water_header_and_data(r.text)
        df = parse_header(usgs_split[0], usgs_split[1], technosphere_flow_array, waste_flow_array)
        
        if not data_frame_dictionary.get(year, None):
            data_frame_dictionary[year] = []

        data_frame_dictionary[year].append(df)
    return data_frame_dictionary


def get_usgs_water_header_and_data(text):
    """This method takes the file data line by line and seperates it into 3 catigories
        Metadata - these are the comments at the begining of the file. 
        Header data - the headers for the data.
        Data - the actuall data for each of the headers. 
        This method also eliminates the table metadata. The table metadata declares how long each string in the table can be."""
    flag = True
    header = ""
    column_size = ""
    data = []
    metadata = []
    with io.StringIO(text) as fp:
        for line in fp:
            if line[0] != '#':
                if flag == True:
                    header = line
                    flag = False
                else:
                    if "16s" in line:
                        column_size = line
                    else:
                        data.append(line)
            else:
                metadata.append(line)
    return (header, data)


def process_data(description_list, unit_list, index_list, flow_name_list, general_list,
                 ActivityProducedBy_list, ActivityConsumedBy_list, compartment_list, flow_type_list,
                 data):
    """This method adds in the data be used for the Flow by activity.
    This method creates a dictionary from the parced headers and the parsed data.
    This dictionary is then turned into a panda data frame and returned."""
    final_class_list = []
    final_source_name_list = []
    final_flow_name_list = []
    final_flow_amount_list = []
    final_unit_list = []
    final_activity_produced_list = []
    final_activity_consumed_list = []
    final_compartment_list = []
    final_fips_list = []
    # final_flow_type_list = []
    final_year_list = []
    final_data_reliability_list = []
    final_data_collection_list = []
    final_description_list = []
    final_measure_of_spread_list = []
    final_spread_list = []
    final_distribution_type_list = []
    final_min_list = []
    final_max_list = []
    all_lists = []

    year_index = general_list.index("year")
    state_cd_index = general_list.index("state_cd")
    county_cd_index = general_list.index("county_cd")
    for d in data:
        data_list = d.split("\t")
        year = data_list[year_index]
        fips = str(data_list[state_cd_index]) + str(data_list[county_cd_index])

        for i in range(len(flow_name_list)):
            data_index = index_list[i]
            data_value = data_list[data_index]
            if data_value.strip() != "-":
                year_value = data_list[year_index]
                final_class_list.append(class_value)
                final_source_name_list.append(source)
                final_flow_name_list.append(flow_name_list[i])
                final_flow_amount_list.append(data_value)
                final_unit_list.append(unit_list[i])
                final_activity_produced_list.append(ActivityProducedBy_list[i])
                final_activity_consumed_list.append(ActivityConsumedBy_list[i])
                final_compartment_list.append(compartment_list[i])
                final_fips_list.append(fips)
                # final_flow_type_list.append(flow_type_list[i])
                final_year_list.append(year_value)
                final_measure_of_spread_list.append(None)
                final_spread_list.append(None)
                final_distribution_type_list.append(None)
                final_min_list.append(None)
                final_max_list.append(None)
                final_data_reliability_list.append(None)
                final_data_collection_list.append("5")  # placeholder DQ score
                final_description_list.append(description_list[i])


    flow_by_activity = []
    for key in flow_by_activity_fields.keys():
        flow_by_activity.append(key)
    dict = {flow_by_activity[0]: final_class_list, 
        flow_by_activity[1]: final_source_name_list, 
        flow_by_activity[2]: final_flow_name_list, 
        flow_by_activity[3]: final_flow_amount_list,
        flow_by_activity[4]: final_unit_list,
        flow_by_activity[5]: final_activity_produced_list,
        flow_by_activity[6]: final_activity_consumed_list,
        flow_by_activity[7]: final_compartment_list, 
        flow_by_activity[8]: final_fips_list,
        flow_by_activity[9]: final_year_list, 
        flow_by_activity[10]: final_measure_of_spread_list,
        flow_by_activity[11]: final_spread_list,
        flow_by_activity[12]: final_distribution_type_list,
        flow_by_activity[13]: final_min_list,
        flow_by_activity[14]: final_max_list,
        flow_by_activity[15]: final_data_reliability_list,
        flow_by_activity[16]: final_data_collection_list,
        flow_by_activity[17]: final_description_list}
    

    df = pd.DataFrame(dict)
    return df

def capitalize_first_letter(string):
    return_string = ""
    split_array = string.split(" ")
    for s in split_array:
        return_string = return_string + " " + s.capitalize() 
    return return_string.strip()

def activity(name):
    name_split = name.split(",")
    if "Irrigation" in name:
        n = name_split[0] + "," + name_split[1]
    else:
        n = name_split[0]

    if " to " in n:
        activity = n.split(" to ")
        name = split_name(activity[0])
        produced = name[0]
        consumed = capitalize_first_letter(activity[1])
    elif " from " in n:
        activity = n.split(" from ")
        name = split_name(activity[0])
        produced = capitalize_first_letter(activity[1])
        consumed = name[0] 
    elif "consumptive" in n:
        split_case = split_name(n)
        consumed = None
        produced = capitalize_first_letter(split_case[0])
    elif ")" in n:
        produced = None
        open_paren_split = n.split("(")
        capitalized_string = capitalize_first_letter(open_paren_split[0])
        close_paren_split = open_paren_split[1].split(")")
        consumed = capitalized_string.strip() + " " + close_paren_split[0].strip()
    elif "total deliveries" in n:
        split_case = split_name(n)
        consumed = None
        produced = capitalize_first_letter(split_case[0])
    else:
        split_case = split_name(n)
        produced = None
        consumed = capitalize_first_letter(split_case[0])
    return (produced, consumed)


def parse_header(headers, data, technosphere_flow_array, waste_flow_array):
    """This method takes the header data and parses it so that it works with the flow by
     activity format. This method creates lists for each object to go along with the
     Flow-By-Activity """
    headers_list = headers.split("\t")
    description_list = []
    general_list = []
    comma_count = []
    index_list = []
    flow_name_list = []
    unit_list = []
    activity_list = []
    flow_type_list = []
    compartment_list = []
    data_frame_list = []
    ActivityProducedBy_list = []
    ActivityConsumedBy_list = []
    index = 0
    for h in headers_list:
        comma_split = h.split(",")
        comma_count.append(len(comma_split))
        if "Commercial" not in h:
            if "Mgal" in h:
                index_list.append(index)
                description_list.append(h)
                unit_split = h.split("in ")
                unit = unit_split[1]
                name = unit_split[0].strip()
                # flow_type_list.append(
                #     determine_flow_type(name, technosphere_flow_array, waste_flow_array))
                compartment_list.append(extract_compartment(name))
                unit_list.append(unit)
                activities = activity(h)
                ActivityProducedBy_list.append(activities[0])
                ActivityConsumedBy_list.append(activities[1])

                if (len(comma_split) == 1):
                    names_split = split_name(name)

                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(h))
                elif (len(comma_split) == 2):
                    name = comma_split[0]
                    if ")" in name:
                        paren_split = name.split(")")
                        activity_name = paren_split[0].strip() + ")"
                        flow_name = paren_split[1].strip()

                        activity_list.append(activity_name)
                        flow_name_list.append(extract_flow_name(h))
                    else:
                        names_split = split_name(name)

                        activity_list.append(names_split[0].strip())
                        flow_name_list.append(extract_flow_name(h))
                elif (len(comma_split) == 3):
                    name = comma_split[0]
                    names_split = split_name(name)

                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(h))
                elif (len(comma_split) == 4):
                    name = comma_split[0] + comma_split[1]
                    names_split = split_name(name)

                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(h))
            else:
                if " in" not in h:
                    if "num" not in h:
                        general_list.append(h)
        index += 1
    data_frame = process_data(description_list, unit_list, index_list, flow_name_list, general_list,
                              ActivityProducedBy_list, ActivityConsumedBy_list, compartment_list,
                              flow_type_list, data)
    return data_frame


def split_name(name):
    """This method splits the header name into a source name and a flow name"""
    space_split = name.split(" ")
    upper_case = ""
    lower_case = ""
    for s in space_split:
        first_letter = s[0]
        if first_letter.isupper():
            upper_case = upper_case.strip() + " " + s
        else:
            lower_case = lower_case.strip() + " " + s
    return (upper_case, lower_case)


def extract_compartment(name):
    """Sets the extract_compartment based on it's name"""
    if "surface" in name.lower():
        compartment = "surface"
    elif "ground" in name.lower():
        compartment = "ground"
    elif "total" in name:
        compartment = "total"
    else:
        compartment = None
    return compartment


def extract_flow_name(name):
    """Sets the flow name based on it's name"""
    if "fresh" in name.lower():
        flow_name = "fresh"
    elif "saline" in name.lower():
        flow_name = "saline"
    else:
        flow_name = None
    return flow_name


if __name__ == '__main__':
    config = load_sourceconfig(source)
    url_list = build_usgs_water_url_list(config)
    df_lists = call_usgs_water_urls(url_list[0:4])
    # Need to check each df before concatenating

    for d in df_lists:
        df = pd.concat(df_lists[d])
        # Assign data quality scores
        df.loc[df['ActivityConsumedBy'].isin(['Public Supply']), 'DataReliability'] = '2'
        df.loc[df['ActivityConsumedBy'].isin(['Aquaculture', 'Livestock', 'Total Thermoelectric Power',
                                              'Thermoelectric Power Once-through cooling',
                                              'Thermoelectric Power Closed-loop cooling',
                                              'Wastewater Treatment']), 'DataReliability'] = '3'
        df.loc[df['ActivityConsumedBy'].isin(['Domestic', 'Industrial', 'Irrigation, Crop', 'Irrigation, Golf Courses',
                                              'Irrigation, Total', 'Mining']), 'DataReliability'] = '4'
        # Modify Unit column
        df.loc[df['Unit'] == 'Mgal/', 'Unit'] = 'Mgal/d'
        log.info("Retrieved data for " + source + " " + d)
        store_flowbyactivity(df, source, d)

