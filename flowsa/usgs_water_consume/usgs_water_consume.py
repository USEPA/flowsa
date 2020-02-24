import sys, os, inspect, io
import requests
import pandas as pd
print(os.path.abspath(os.getcwd()))
from data_reshape import *
#usgs_water_consume.data_reshape import *
#from flowsa.usgs_water_consume.data_reshape import parse_header
from datapull import get_yaml, print_file


# activity_array =  ["Total population","Public supply","Domestic","Industrial","Total Thermoelectric Power","Fossil-fuel thermoelectric power","Geothermal thermoelectric power","Nuclear thermoelectric power","Thermoelectric power (once-through cooling)","Thermoelectric power (closed-loop cooling)","Mining","Livestock","Livestock (stock)","Livestock (animal specialties)","Aquaculture","Irrigation, total","Irrigation, crop","Irrigation, golf courses","Hydroelectric power","Wastewater treatment"]
# surface_array =  ["groundwater","surface", "total"]
# water_type_array = ["fresh", "saline"]
technosphere_flow_array = ["consumptive"]
waste_flow_array = ["wastewater", "loss"]


def parse_yaml_url(documents):
        for key, value in documents.items():
            if(key=="url"):
                url_list = []
                states = value["states"] 
                base_url = value["base_url"]
                url_path = value["url_path"]
                param_format = "format=" + str(value["format"])
                param_compression = "_compression=" +str(value["_compression"])
                param_wu_area = "&wu_area=" + str(value["wu_area"])
                param_wu_year = "&wu_year=" + str(value["wu_year"])
                param_wu_county = "&wu_county=" + str(value["wu_county"])
                param_wu_category = "&wu_category=" + str(value["wu_category" ])
                for s in states:
                    url = "{0}{1}{2}{3}{4}{5}{6}{7}{8}".format(base_url, s, url_path, param_format, param_compression, param_wu_area, param_wu_year, param_wu_county, param_wu_category)
                    url_list.append(url)
        return url_list

def append_data_frames(data_frames_list):
    result = pd.concat(data_frames_list)
    print_file(result)

    
def call_urls(urls_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    request_list = []
    data_frames_list = []
    for url in urls_list:
        r = requests.get(url)
        usgs_split = get_header_general_and_data(r.text)
        df = parse_header(usgs_split[0], usgs_split[1], technosphere_flow_array, waste_flow_array)
        data_frames_list.append(df)
    append_data_frames(data_frames_list)

def get_header_general_and_data(text):
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

document = get_yaml()
url_list = parse_yaml_url(document)
call_urls(url_list)