# data_reshape.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import pandas as pd
"""
Classes and methods for reshaping pulled data.

Available functions:
process_data
split_name
compartment
activity
flow_type
parse_header
- 
"""

def process_data(headers_water_only, unit_list, index_list, source_name_list, flow_name_list,general_list,  activity_list, compartment_list, flow_type_list,data):
     """This method adds in the data be used for the Flow by activity."""
     year_index = general_list.index("year")
     state_cd_index = general_list.index("state_cd")
     county_cd_index = general_list.index("county_cd")
     flow_amount_list = []
     year_list = []
     fips_list = []
     reliability_score_list = []
     data_collection_list = []
     for d in data:
        data_list = d.split("\t")    
        year = data_list[year_index]
        fips = str(data_list[state_cd_index]) + str(data_list[county_cd_index])
       
        for i in range(len(source_name_list)):
            flow_amount_list.append(data_list[index_list[i]])
            year_list.append(year)
            fips_list.append(fips)
            reliability_score_list.append("null")
            data_collection_list.append("null")
            print(flow_amount_list)
        print(len(flow_amount_list))
    #  dict = {'source name': source_name_list, 'flow name': flow_name_list, 'flow amount': flow_amount_list, 'unit': unit_list, 'activity': activity_list, 'compartment': compartment_list, 'FIPS': fips_list, 'flow type': flow_type_list, 'year': year_list, 'reliability score': reliability_score_list, 'data collection': data_collection_list}
    #  df = pd.DataFrame(dict)
    #  return df
       
        # for i in range(len(source_name_list)):
           

        #     unit = unit_list[i]
        #     activity = activity_list[i]
        #     reliablity_score = "null"
        #     data_collection = "null"
        #     compartment = compartment_list[i]
        #     flow_type = flow_type_list[i]
        #     description = headers_water_only[i]
        #     print(description, source_name, flow_name, flow_amount, unit, activity, reliablity_score, data_collection, compartment, fips, flow_type, year)

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

def compartment(name):
    """This method saves compartment to water. """
    compartment = "water"
    return compartment

def activity(name,  activity_array, surface_array, water_type_array):
    """Sets the activity based on the name"""
    for a in activity_array:
        at = a.lower()
        if at in name.lower():
            activity = a
            for s in surface_array:
                if s in name.lower():
                    activity = activity + " " + s
            for w in water_type_array:
                if w in name.lower():
                    activity = activity + " " + w
            return activity

def flow_type(name, technosphere_flow_array, waste_flow_array):       
    """Takes the header and assigns one of three flow types""" 
    technosphere_flow_array = ["consumptive"]
    waste_flow_array = ["wastewater", "loss"]
    flow_type = "ELEMENTARY_FLOW"
    for t in technosphere_flow_array:
        if t in name:
            flow_type = "TECHNOSPHERE_FLOW"
    for w in waste_flow_array:
        if w in name:
             flow_type = "WASTE_FLOW"
    return flow_type

def parse_header(headers, data, activity_array, surface_array, water_type_array, technosphere_flow_array, waste_flow_array):
    """This method takes the header data and parses it so that it works with the flow by activity format. 
    This method creates lists for each object to go along with the Flow-By-Activity
   headers-usgs data headers
   data - usgs data """
    headers_list = headers.split("\t")
    headers_water_only = []
    general_list = []
    comma_count = []
    names_list = []
    index_list = []
    source_name_list = []
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
                activity_list.append(activity(name, activity_array, surface_array, water_type_array))
                flow_type_list.append(flow_type(name, technosphere_flow_array, waste_flow_array))
                compartment_list.append(compartment(name))
                unit_list.append(unit)
                if(len(comma_split) == 1):
                    names_split = split_name(name)
                    source_name_list.append(names_split[0].strip())
                    flow_name_list.append(names_split[1].strip())
                elif(len(comma_split) == 2):
                    name = comma_split[0]
                    if ")" in name:
                        paren_split = name.split(")")
                        source_name = paren_split[0].strip() + ")"
                        flow_name = paren_split[1].strip()
                        source_name_list.append(source_name)
                        flow_name_list.append(flow_name)
                    else:
                        names_split = split_name(name)
                        source_name_list.append(names_split[0].strip())
                        flow_name_list.append(names_split[1].strip())
                elif(len(comma_split) == 3):
                    name = comma_split[0]
                    names_split = split_name(name)
                    source_name_list.append(names_split[0].strip())
                    flow_name_list.append(names_split[1].strip())
                elif(len(comma_split) == 4):
                    name = comma_split[0] + comma_split[1]
                    names_split = split_name(name)
                    source_name_list.append(names_split[0].strip())
                    flow_name_list.append(names_split[1].strip())
            else:
                if " in" not in h:
                    if "num" not in h:
                        general_list.append(h)
        index += 1
    
    data_frame = process_data(headers_water_only, unit_list, index_list, source_name_list, flow_name_list,general_list,  activity_list, compartment_list, flow_type_list, data)
    return data_frame

def save_data(data_frame):
    df = pd.DataFrame(data={'Class': [1, 2], 'SourceName': [1, 2], 'FlowName': [3, 4], 'FlowAmount': [3, 4], 'Unit': [3, 4], 'Activity': [3, 4], 'Compartment': [3, 4], 'FIPS': [3, 4], 'FlowType': [3, 4], 'Year': [3, 4], 'DataReliability': [3, 4], 'DataCollection': [3, 4]})
