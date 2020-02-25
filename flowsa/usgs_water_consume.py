# usgs_water_consume.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
import io
import pandas as pd
from flowsa.datapull import load_sourceconfig, store_flowbyactivity, make_http_request
from flowsa.common import log

source = 'usgs_water_consume'

# activity_array =  ["Total population","Public supply","Domestic","Industrial","Total Thermoelectric Power","Fossil-fuel thermoelectric power","Geothermal thermoelectric power","Nuclear thermoelectric power","Thermoelectric power (once-through cooling)","Thermoelectric power (closed-loop cooling)","Mining","Livestock","Livestock (stock)","Livestock (animal specialties)","Aquaculture","Irrigation, total","Irrigation, crop","Irrigation, golf courses","Hydroelectric power","Wastewater treatment"]
# surface_array =  ["groundwater","surface", "total"]
# water_type_array = ["fresh", "saline"]
technosphere_flow_array = ["consumptive", "Public Supply"]
waste_flow_array = ["wastewater", "loss"]


def build_usgs_water_url_list(config):
    """
    
    :param config: 
    :return: 
    """
    for k,v in config.items():
        if (k == "url"):
            url_list = []
            states = v["states"]
            base_url = v["base_url"]
            url_path = v["url_path"]
            param_format = "format=" + str(v["format"])
            param_compression = "_compression=" + str(v["_compression"])
            param_wu_area = "&wu_area=" + str(v["wu_area"])
            param_wu_year = "&wu_year=" + str(v["wu_year"])
            param_wu_county = "&wu_county=" + str(v["wu_county"])
            param_wu_category = "&wu_category=" + str(v["wu_category"])
            for s in states:
                url = "{0}{1}{2}{3}{4}{5}{6}{7}{8}".format(base_url, s, url_path, param_format,
                                                           param_compression, param_wu_area, param_wu_year,
                                                           param_wu_county, param_wu_category)
                url_list.append(url)
    return url_list
    
def call_usgs_water_urls(url_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in url_list:
        r = make_http_request(url)
        usgs_split = get_usgs_water_header_and_data(r.text)
        df = parse_header(usgs_split[0], usgs_split[1], technosphere_flow_array, waste_flow_array)
        data_frames_list.append(df)
    return data_frames_list

def get_usgs_water_header_and_data(text):
    """This method takes the file data line by line and seperates it into 3 catigories
        Metadata - these are the comments at the begining of the file. 
        Header data - the headers for the data.
        Data - the actuall data for each of the headers. 
        This method also eliminates the table metadata. The table metadata declares how long each string in the table can be."""
    flag = True
    header = "" 
    column_size = ""
    data =[]
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


def process_data(headers_water_only, unit_list, index_list,  flow_name_list,general_list,  activity_list, compartment_list, flow_type_list,data):
     """This method adds in the data be used for the Flow by activity.
     This method creates a dictionary from the parced headers and the parsed data.
     This dictionary is then turned into a panda data frame and returned."""
     final_class_list = []
     final_source_name_list = []
     final_flow_name_list = []
     final_flow_amount_list = []
     final_unit_list = []
     final_activity_list = []
     final_compartment_list = []
     final_fips_list = []
     final_flow_type_list = []
     final_year_list = []
     final_data_reliability_list = []
     final_data_collection_list = []
     final_description_list = []

     year_index = general_list.index("year")
     state_cd_index = general_list.index("state_cd")
     county_cd_index = general_list.index("county_cd")
     for d in data:
        data_list = d.split("\t")
        year = data_list[year_index]
        fips = str(data_list[state_cd_index]) + str(data_list[county_cd_index])

        for i in range(len(activity_list)):
            data_index = index_list[i]
            data_value = data_list[data_index]
            if data_value == "-":
                data_value = None
            year_value = data_list[year_index]
            final_class_list.append( "water")
            final_source_name_list.append("usgs_water_consume")
            final_flow_name_list.append(flow_name_list[i])
            final_flow_amount_list.append(data_value)
            final_unit_list.append(unit_list[i])
            final_activity_list.append(activity_list[i])
            final_compartment_list.append(compartment_list[i])
            final_fips_list.append(fips)
            final_flow_type_list.append(flow_type_list[i])
            final_year_list.append(year_value)
            final_data_reliability_list.append("null")
            final_data_collection_list.append("null")
            final_description_list.append(headers_water_only[i])
     #flow_by_activity_fields(final_class_list, final_source_name_list, final_flow_name_list, final_flow_amount_list, final_unit_list, final_activity_list, final_compartment_list, final_fips_list, final_flow_type_list, final_year_list, final_data_reliability_list, final_data_collection_list)
     dict = {'Class': final_class_list, 'Source Name': final_source_name_list, 'Flow Name': final_flow_name_list, 'Flow Amount': final_flow_amount_list,
      'Unit': final_unit_list, 'Activity': final_activity_list, 'Compartment': final_compartment_list, 'FIPS': final_fips_list, 'Flow Type': final_flow_type_list,
      'Year': final_year_list, 'Reliability Score': final_data_reliability_list, 'Data Collection': final_data_collection_list, 'Description':final_description_list}
     df = pd.DataFrame(dict)
     return df

def parse_header(headers, data,  technosphere_flow_array, waste_flow_array):
    """This method takes the header data and parses it so that it works with the flow by activity format.
    This method creates lists for each object to go along with the Flow-By-Activity """
    headers_list = headers.split("\t")
    headers_water_only = []
    general_list = []
    comma_count = []
    index_list = []
    flow_name_list = []
    unit_list = []
    activity_list = []
    flow_type_list = []
    compartment_list = []
    data_frame_list = []
    index = 0
    for h in headers_list:
        comma_split = h.split(",")
        comma_count.append(len(comma_split))
        if "Commercial" not in h:
            if "Mgal"in h:
                index_list.append(index)
                headers_water_only.append(h)
                unit_split = h.split("in ")
                unit = unit_split[1]
                name = unit_split[0].strip()
                flow_type_list.append(determine_flow_type(name, technosphere_flow_array, waste_flow_array))
                compartment_list.append(extract_compartment(name))
                unit_list.append(unit)
                if(len(comma_split) == 1):
                    names_split = split_name(name)
                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(name))
                elif(len(comma_split) == 2):
                    name = comma_split[0]
                    if ")" in name:
                        paren_split = name.split(")")
                        activity_name = paren_split[0].strip() + ")"
                        flow_name = paren_split[1].strip()
                        activity_list.append(activity_name)
                        flow_name_list.append(extract_flow_name(name))
                    else:
                        names_split = split_name(name)
                        activity_list.append(names_split[0].strip())
                        flow_name_list.append(extract_flow_name(name))
                elif(len(comma_split) == 3):
                    name = comma_split[0]
                    names_split = split_name(name)
                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(name))
                elif(len(comma_split) == 4):
                    name = comma_split[0] + comma_split[1]
                    names_split = split_name(name)
                    activity_list.append(names_split[0].strip())
                    flow_name_list.append(extract_flow_name(name))
            else:
                if " in" not in h:
                    if "num" not in h:
                        general_list.append(h)
        index += 1

    data_frame = process_data(headers_water_only, unit_list, index_list, flow_name_list,general_list,  activity_list, compartment_list, flow_type_list, data)
    return data_frame

def split_name(name):
    """This method splits the header name into a source name and a flow name"""
    space_split = name.split(" ")
    source_name = ""
    flow_name = ""
    for s in space_split:
        first_letter = s[0]
        if first_letter.isupper():
            source_name = source_name.strip() + " " + s
        else:
            flow_name = flow_name.strip() + " " + s
    return(source_name, flow_name)

def extract_compartment(name):
    """Sets the extract_compartment based on it's name"""
    if"surface" in name.lower():
        compartment = "surface"
    elif "ground" in name.lower():
        compartment = "ground"
    else:
        compartment = "blank"
    return compartment

def extract_flow_name(name):
    """Sets the flow name based on it's name"""
    if"fresh" in name.lower():
        flow_name = "fresh"
    elif "saline" in name.lower():
        flow_name = "saline"
    else:
        flow_name = None
    return flow_name

def determine_flow_type(name, technosphere_flow_array, waste_flow_array):
    """Takes the header and assigns one of three flow types.
    Everything starts as elementry but if there are keywords for technosphere and waste flow.
    The keywords are set in in the usgs_water_consume.yaml """
    flow_type = "ELEMENTARY_FLOW"
    for t in technosphere_flow_array:
        if t in name:
            flow_type = "TECHNOSPHERE_FLOW"
    for w in waste_flow_array:
        if w in name:
             flow_type = "WASTE_FLOW"
    return flow_type

if __name__ == '__main__':
    config = load_sourceconfig(source)
    url_list = build_usgs_water_url_list(config)
    df_list = call_usgs_water_urls(url_list[0:1])
    #Need to check each df before concatenating
    df = pd.concat(df_list)
    log.info("Retrieved data for "+source)
    store_flowbyactivity(df)
