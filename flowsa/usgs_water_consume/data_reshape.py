# data_reshape.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import pandas as pd
"""
Classes and methods for reshaping pulled data.

Available functions:
process_data - takes the lists that are dirived from the header and the data. 
                        It then parses the data and puts it into a data table the data table. 
                        The data table is formated into a panda data frame so it can be formated into a parquet file later.  
split_name - splits the name into source and flow name
compartment - sets the compartment string 
activity - sets the activity string
flow_type - sets the flow type
parse_header - takes the header and parses it into several lists for the Flow by activity format. 
- 
"""

def process_data(headers_water_only, unit_list, index_list, source_name_list, flow_name_list,general_list,  activity_list, compartment_list, flow_type_list,data):
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
       
        for i in range(len(source_name_list)):
            data_index = index_list[i]
            data_value = data_list[data_index]
           
            final_class_list.append(compartment_list[i]) 
            final_source_name_list.append(source_name_list[i])
            final_flow_name_list.append(flow_name_list[i])
            final_flow_amount_list.append(data_value)
            final_unit_list.append(unit_list[i])
            final_activity_list.append(activity_list[i])
            final_compartment_list.append(compartment_list[i])
            final_fips_list.append(fips)
            final_flow_type_list.append(flow_type_list[i])
            final_year_list.append(year)
            final_data_reliability_list.append("null")
            final_data_collection_list.append("null")
            final_description_list.append(headers_water_only[i])

     dict = {'Class': final_class_list, 'Source Name': final_source_name_list, 'Flow Name': final_flow_name_list, 'Flow Amount': final_flow_amount_list,
      'Unit': final_unit_list, 'Activity': final_activity_list, 'Compartment': final_compartment_list, 'FIPS': final_fips_list, 'Flow Type': final_flow_type_list, 
      'Year': final_year_list, 'Reliability Score': final_data_reliability_list, 'Data Collection': final_data_collection_list, 'Description':final_description_list}
     df = pd.DataFrame(dict)
     return df

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
    """This method saves compartment to water. This is also currently being used for Class as well."""
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
    """Takes the header and assigns one of three flow types. 
    Everything starts as elementry but if there are keywords for technosphere and waste flow. 
    The keywords are set in in the datasource_config.yaml """ 
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
    This method creates lists for each object to go along with the Flow-By-Activity """
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