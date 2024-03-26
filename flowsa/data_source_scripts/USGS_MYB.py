import io
import math
import numpy as np
import pandas as pd
from string import digits
from flowsa.flowsa_log import log
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS

YEARS_COVERED = {
    "asbestos": "2014-2018",
    "barite": "2014-2018",
    "bauxite": "2013-2017",
    "beryllium": "2014-2018",
    "boron": "2014-2018",
    "chromium": "2014-2018",
    "clay": "2015-2016",
    "cobalt": "2013-2017",
    "copper": "2011-2015",
    "diatomite": "2014-2018",
    "feldspar": "2013-2017",
    "fluorspar": "2013-2017",
    "fluorspar_inports": ["2016", "2017"],
    "gallium": "2014-2018",
    "garnet": "2014-2018",
    "gold": "2013-2017",
    "graphite": "2013-2017",
    "gypsum": "2014-2018",
    "iodine": "2014-2018",
    "ironore": "2014-2018",
    "kyanite": "2014-2018",
    "lead": "2012-2018",
    "lime": "2014-2018",
    "lithium": "2013-2017",
    "magnesium": "2013-2017",
    "manganese": "2012-2016",
    "manufacturedabrasive": "2017-2018",
    "mica": "2014-2018",
    "molybdenum": "2014-2018",
    "nickel": "2012-2016",
    "niobium": "2014-2018",
    "peat": "2014-2018",
    "perlite": "2013-2017",
    "phosphate": "2014-2019",
    "platinum": "2014-2018",
    "potash": "2014-2019",
    "pumice": "2014-2018",
    "rhenium": "2014-2018",
    "salt": "2013-2017",
    "sandgravelconstruction": "2013-2017",
    "sandgravelindustrial": "2014-2018",
    "silver": "2012-2016",
    "sodaash": "2010-2017",
    "sodaash_t4": ["2016", "2017"],
    "stonecrushed": "2014-2018",
    "stonedimension": "2013-2017",
    "strontium": "2014-2018",
    "talc": "2013-2017",
    "titanium": "2013-2017",
    "tungsten": "2013-2017",
    "vermiculite": "2014-2018",
    "zeolites": "2014-2018",
    "zinc": "2013-2017",
    "zirconium": "2013-2017",
}


def usgs_myb_year(years, current_year_str):
    """
    Sets the column for the string based on the year. Checks that the year
    you picked is in the last file.
    :param years: string, with hypthon
    :param current_year_str: string, year of interest
    :return: string, year
    """
    years_array = years.split("-")
    lower_year = int(years_array[0])
    upper_year = int(years_array[1])
    current_year = int(current_year_str)
    if lower_year <= current_year <= upper_year:
        column_val = current_year - lower_year + 1
        return "year_" + str(column_val)
    else:
        log.info(f"Your year is out of scope. Pick a year between "
                 f"{lower_year} and {upper_year}")


def usgs_myb_name(USGS_Source):
    """
    Takes the USGS source name and parses it so it can be used in other parts
    of Flow by activity.
    :param USGS_Source: string, usgs source name
    :return:
    """
    source_split = USGS_Source.split("_")
    name_cc = str(source_split[2])
    name = ""
    for char in name_cc:
        if char.isupper():
            name = name + " " + char
        else:
            name = name + char
    name = name.lower()
    name = name.strip()
    return name


def usgs_myb_static_variables():
    """
    Populates the data values for Flow by activity that are the same
    for all of USGS_MYB Files
    :return:
    """
    data = {}
    data["Class"] = "Geological"
    data['FlowType'] = "ELEMENTARY_FLOW"
    data["Location"] = US_FIPS
    data["Compartment"] = "ground"
    data["Context"] = None
    data["ActivityConsumedBy"] = None
    return data


def usgs_myb_remove_digits(value_string):
    """
    Eliminates numbers in a string
    :param value_string:
    :return:
    """
    remove_digits = str.maketrans('', '', digits)
    return_string = value_string.translate(remove_digits)
    return return_string


def usgs_myb_url_helper(*, build_url, config, year, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """

    # Replace year-dependent aspects of file url
    url = (build_url
           .replace('__FILENAME__', config.get('filename_replacement',
                                               {}).get(int(year), 'NULL'))
           .replace('__YEAR__', year)
           .replace('__FORMAT__', config.get('file_format',
                                             {}).get(int(year), 'NULL'))
           )

    return [url]


def usgs_asbestos_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[4:11]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 12:
        for x in range(12, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Unit", "space_1", "year_1",
                           "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['asbestos'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_asbestos_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    product = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['asbestos'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Exports and reexports:":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['asbestos'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "nan":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(dataframe,
                                                        str(year))
    return dataframe


def usgs_barite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(
        io.BytesIO(resp.content), sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:14]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['barite'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_barite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['barite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:3":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Crude, sold or used by producers:":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports:2":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['barite'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_bauxite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:14]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_2",  "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['bauxite'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_bauxite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Total"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['bauxite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, as shipped:":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == \
                    "Exports, as shipped:":
                prod = "export"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"

                flow_amount = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = flow_amount
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_beryllium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T4')

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[6:9]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]
    df_data_2 = pd.DataFrame(df_raw_data.loc[12:12]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_2.columns) > 11:
        for x in range(11, len(df_data_2.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_2[col_name]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]
    if len(df_data_2. columns) == 11:
        df_data_2.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['beryllium'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_beryllium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["United States6", "Mine shipments1",
                  "Imports for consumption, beryl2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['beryllium'], year)
    for df in df_list:

        for index, row in df.iterrows():
            prod = "production"
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, beryl2":
                prod = "imports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["Description"] = name
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_boron_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data.loc[8:8]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data.loc[21:22]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_data_three = pd.DataFrame(df_raw_data.loc[27:28]).reindex()
    df_data_three = df_data_three.reset_index()
    del df_data_three["index"]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]
        df_data_three.columns = ["Production", "space_1", "year_1", "space_2",
                                 "year_2", "space_3", "year_3", "space_4",
                                 "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['boron'], year))
    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]
            del df_data_three[col]

    frames = [df_data_one, df_data_two, df_data_three]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_boron_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["B2O3 content", "Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['boron'], year)

    for df in df_list:
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "B2O3 content" or \
                    df.iloc[index]["Production"].strip() == "Quantity":
                product = "production"

            if df.iloc[index]["Production"].strip() == "Colemanite:4":
                des = "Colemanite"
            elif df.iloc[index]["Production"].strip() == "Ulexite:4":
                des = "Ulexite"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                if des == name:
                    data['FlowName'] = name + " " + product
                else:
                    data['FlowName'] = name + " " + product + " " + des
                data["Description"] = des
                data["ActivityProducedBy"] = name
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_chromium_call(*, resp, year, **_):
    """"
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[4:24]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Unit", "space_1", "year_1",
                           "space_2", "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5"]
    elif len(df_data. columns) == 13:
        df_data.columns = ["Production", "Unit", "space_1", "year_1",
                           "space_2", "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5", "space_6"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['chromium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_chromium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Secondary2", "Total"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['chromium'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Imports:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Secondary2":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['chromium'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_clay_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data_ball = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                              sheet_name='T3')
    df_data_ball = pd.DataFrame(df_raw_data_ball.loc[19:19]).reindex()
    df_data_ball = df_data_ball.reset_index()
    del df_data_ball["index"]

    df_raw_data_bentonite = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                   sheet_name='T4 ')
    df_data_bentonite = pd.DataFrame(
        df_raw_data_bentonite.loc[28:28]).reindex()
    df_data_bentonite = df_data_bentonite.reset_index()
    del df_data_bentonite["index"]

    df_raw_data_common = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T5 ')
    df_data_common = pd.DataFrame(df_raw_data_common.loc[40:40]).reindex()
    df_data_common = df_data_common.reset_index()
    del df_data_common["index"]

    df_raw_data_fire = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                              sheet_name='T6 ')
    df_data_fire = pd.DataFrame(df_raw_data_fire.loc[12:12]).reindex()
    df_data_fire = df_data_fire.reset_index()
    del df_data_fire["index"]

    df_raw_data_fuller = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T7 ')
    df_data_fuller = pd.DataFrame(df_raw_data_fuller.loc[17:17]).reindex()
    df_data_fuller = df_data_fuller.reset_index()
    del df_data_fuller["index"]

    df_raw_data_kaolin = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T8 ')
    df_data_kaolin = pd.DataFrame(df_raw_data_kaolin.loc[18:18]).reindex()
    df_data_kaolin = df_data_kaolin.reset_index()
    del df_data_kaolin["index"]

    df_raw_data_export = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T13')
    df_data_export = pd.DataFrame(df_raw_data_export.loc[6:15]).reindex()
    df_data_export = df_data_export.reset_index()
    del df_data_export["index"]

    df_raw_data_import = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T14')
    df_data_import = pd.DataFrame(df_raw_data_import.loc[6:13]).reindex()
    df_data_import = df_data_import.reset_index()
    del df_data_import["index"]

    df_data_ball.columns = ["Production", "space_1", "year_1", "space_2",
                            "value_1", "space_3", "year_2", "space_4",
                            "value_2"]
    df_data_bentonite.columns = ["Production", "space_1", "year_1", "space_2",
                                 "value_1", "space_3", "year_2", "space_4",
                                 "value_2"]
    df_data_common.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_fire.columns = ["Production", "space_1", "year_1", "space_2",
                            "value_1", "space_3", "year_2", "space_4",
                            "value_2"]
    df_data_fuller.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_kaolin.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_export.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2", "space_5", "extra"]
    df_data_import.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2", "space_5", "extra"]

    df_data_ball["type"] = "Ball clay"
    df_data_bentonite["type"] = "Bentonite"
    df_data_common["type"] = "Common clay"
    df_data_fire["type"] = "Fire clay"
    df_data_fuller["type"] = "Fuller’s earth"
    df_data_kaolin["type"] = "Kaolin"
    df_data_export["type"] = "export"
    df_data_import["type"] = "import"

    col_to_use = ["Production", "type"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['clay'], year))
    for col in df_data_import.columns:
        if col not in col_to_use:
            del df_data_import[col]
            del df_data_export[col]

    for col in df_data_ball.columns:
        if col not in col_to_use:
            del df_data_ball[col]
            del df_data_bentonite[col]
            del df_data_common[col]
            del df_data_fire[col]
            del df_data_fuller[col]
            del df_data_kaolin[col]

    frames = [df_data_import, df_data_export, df_data_ball, df_data_bentonite,
              df_data_common, df_data_fire, df_data_fuller, df_data_kaolin]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_clay_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Ball clay", "Bentonite", "Fire clay", "Kaolin",
                  "Fuller’s earth", "Total", "Grand total",
                  "Artificially activated clay and earth",
                  "Clays, not elsewhere classified",
                  "Clays, not elsewhere classified"]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["type"].strip() == "import":
                product = "imports"
            elif df.iloc[index]["type"].strip() == "export":
                product = "exports"
            else:
                product = "production"

            if str(df.iloc[index]["Production"]).strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                if product == "production":
                    data['FlowName'] = \
                        df.iloc[index]["type"].strip() + " " + product
                    data["Description"] = df.iloc[index]["type"].strip()
                    data["ActivityProducedBy"] = df.iloc[index]["type"].strip()
                else:
                    data['FlowName'] = \
                        df.iloc[index]["Production"].strip() + " " + product
                    data["Description"] = df.iloc[index]["Production"].strip()
                    data["ActivityProducedBy"] = \
                        df.iloc[index]["Production"].strip()

                col_name = usgs_myb_year(YEARS_COVERED['clay'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)" or \
                        str(df.iloc[index][col_name]) == "(2)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_cobalt_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T8')
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data_two.loc[6:11]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[23:23]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_2.columns) > 11:
        for x in range(11, len(df_data_2.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_2[col_name]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "space_6", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]
    if len(df_data_2. columns) == 11:
        df_data_2.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['cobalt'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_cobalt_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    name = usgs_myb_name(source)
    des = name
    row_to_use = ["United Statese, 16, 17", "Mine productione",
                  "Imports for consumption", "Exports"]
    dataframe = pd.DataFrame()
    for df in df_list:

        for index, row in df.iterrows():
            prod = "production"
            if df.iloc[index]["Production"].strip() == \
                    "United Statese, 16, 17":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports":
                prod = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()

                data["SourceName"] = source
                data["Year"] = str(year)

                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['cobalt'], year)
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                remove_rows = ["(18)", "(2)"]
                if data["FlowAmount"] not in remove_rows:
                    dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                    dataframe = assign_fips_location_system(
                        dataframe, str(year))
    return dataframe


def usgs_copper_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data.loc[12:12]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[30:31]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production", "Unit"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['copper'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_copper_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index][
                "Production"].strip().translate(remove_digits)
            data = usgs_myb_static_variables()
            data["SourceName"] = source
            data["Year"] = str(year)
            if product == "Total":
                prod = "production"
            elif product == "Exports, refined":
                prod = "exports"
            elif product == "Imports, refined":
                prod = "imports"

            data["ActivityProducedBy"] = "Copper; Mine"
            data['FlowName'] = name + " " + prod
            data["Unit"] = "Metric Tons"
            col_name = usgs_myb_year(YEARS_COVERED['copper'], year)
            data["Description"] = "Copper; Mine"
            data["FlowAmount"] = str(df.iloc[index][col_name])
            dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
            dataframe = assign_fips_location_system(
                dataframe, str(year))
    return dataframe


def usgs_diatomite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[7:10]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) == 10:
        df_data_one.columns = ["Production", "year_1", "space_2", "year_2",
                               "space_3", "year_3", "space_4", "year_4",
                               "space_5", "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['diatomite'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_diatomite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Exports2", "Imports for consumption2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports2":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption2":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()

                data["SourceName"] = source
                data["Year"] = str(year)

                data["Unit"] = "Thousand metric tons"
                col_name = usgs_myb_year(YEARS_COVERED['diatomite'], year)
                data["FlowAmount"] = str(df.iloc[index][col_name])

                data["Description"] = name
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_feldspar_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_two = pd.DataFrame(df_raw_data_two.loc[4:8]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_data_one = pd.DataFrame(df_raw_data_two.loc[10:15]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_two. columns) == 13:
        df_data_two.columns = ["Production", "space_1",  "unit",  "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]
        df_data_one.columns = ["Production", "space_1",  "unit",  "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['feldspar'], year))

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]
            del df_data_one[col]

    frames = [df_data_two, df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_feldspar_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Quantity3"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports, feldspar:4":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:4":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Production, feldspar:e, 2":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == "Nepheline syenite:":
                prod = "production"
                des = "Nepheline syenite"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['feldspar'], year)
                data["FlowAmount"] = str(df.iloc[index][col_name])
                data["Description"] = des
                data["ActivityProducedBy"] = name
                if name == des:
                    data['FlowName'] = name + " " + prod
                else:
                    data['FlowName'] = name + " " + prod + " " + des
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_fluorspar_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    if year in YEARS_COVERED['fluorspar_inports']:
        df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                 sheet_name='T2')
        df_raw_data_three = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                   sheet_name='T7')
        df_raw_data_four = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                  sheet_name='T8')

    df_data_one = pd.DataFrame(df_raw_data_one.loc[5:15]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]
    if year in YEARS_COVERED['fluorspar_inports']:
        df_data_two = pd.DataFrame(df_raw_data_two.loc[7:8]).reindex()
        df_data_three = pd.DataFrame(df_raw_data_three.loc[19:19]).reindex()
        df_data_four = pd.DataFrame(df_raw_data_four.loc[11:11]).reindex()
        if len(df_data_two.columns) == 13:
            df_data_two.columns = ["Production", "space_1", "not_1", "space_2",
                                   "not_2", "space_3", "not_3", "space_4",
                                   "not_4", "space_5", "year_4", "space_6",
                                   "year_5"]
        if len(df_data_three.columns) == 9:
            df_data_three.columns = ["Production", "space_1", "year_4",
                                     "space_2", "not_1", "space_3", "year_5",
                                     "space_4", "not_2"]
            df_data_four.columns = ["Production", "space_1", "year_4",
                                    "space_2", "not_1", "space_3", "year_5",
                                    "space_4", "not_2"]

    if len(df_data_one. columns) == 13:
        df_data_one.columns = ["Production", "space_1",  "unit",  "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['fluorspar'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
    if year in YEARS_COVERED['fluorspar_inports']:
        for col in df_data_two.columns:
            if col not in col_to_use:
                del df_data_two[col]
        for col in df_data_three.columns:
            if col not in col_to_use:
                del df_data_three[col]
        for col in df_data_four.columns:
            if col not in col_to_use:
                del df_data_four[col]
    df_data_one["type"] = "data_one"

    if year in YEARS_COVERED['fluorspar_inports']:
        # aluminum fluoride
        # cryolite
        df_data_two["type"] = "data_two"
        df_data_three["type"] = "Aluminum Fluoride"
        df_data_four["type"] = "Cryolite"
        frames = [df_data_one, df_data_two, df_data_three, df_data_four]
    else:
        frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_fluorspar_parse(*, df_list, source, year, **_):
    """
     Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Quantity3", "Total", "Hydrofluoric acid",
                  "Metallurgical", "Production"]
    prod = ""
    name = usgs_myb_name(source)
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:3":
                prod = "exports"
                des = name
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:3":
                prod = "imports"
                des = name
            elif df.iloc[index]["Production"].strip() == "Fluorosilicic acid:":
                prod = "production"
                des = "Fluorosilicic acid:"

            if str(df.iloc[index]["type"]).strip() == "data_two":
                prod = "imports"
                des = df.iloc[index]["Production"].strip()
            elif str(df.iloc[index]["type"]).strip() == \
                    "Aluminum Fluoride" or \
                    str(df.iloc[index]["type"]).strip() == "Cryolite":
                prod = "imports"
                des = df.iloc[index]["type"].strip()

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['fluorspar'], year)
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_gallium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[5:7]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 11:
        for x in range(11, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data.columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['gallium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_gallium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production, primary crude", "Metal"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['gallium'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Production, primary crude":
                product = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Kilograms"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['gallium'], year)
                if str(df.iloc[index][col_name]).strip() == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "nan":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_garnet_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_two = pd.DataFrame(df_raw_data_two.loc[4:5]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_data_one = pd.DataFrame(df_raw_data_two.loc[10:14]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 13:
        for x in range(13, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]
            del df_data_two[col_name]

    if len(df_data_two. columns) == 13:
        df_data_two.columns = ["Production", "space_1",  "unit",  "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]
        df_data_one.columns = ["Production", "space_1",  "unit",  "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['garnet'], year))

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]
            del df_data_one[col]

    frames = [df_data_two, df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_garnet_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:2":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption: 3":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Crude production:":
                prod = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['garnet'], year)
                data["FlowAmount"] = str(df.iloc[index][col_name])
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_gold_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[6:14]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) == 13:
        df_data.columns = ["Production", "Space", "Units", "space_1",
                           "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['gold'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_gold_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Exports, refined bullion",
                  "Imports for consumption, refined bullion"]
    dataframe = pd.DataFrame()
    product = "production"
    name = usgs_myb_name(source)
    des = name
    for df in df_list:
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "Quantity":
                product = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Exports, refined bullion":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, refined bullion":
                product = "imports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "kilograms"
                data['FlowName'] = name + " " + product

                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['gold'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_graphite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[5:9]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 13:
        df_data.columns = ["Production", "space_1", "Unit", "space_6",
                           "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['graphite'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_graphite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantiy", "Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['graphite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['graphite'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "nan":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_gypsum_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_one = pd.DataFrame(df_raw_data_one.loc[7:10]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]

    if len(df_data_one.columns) == 11:
        df_data_one.columns = ["Production", "space_1",  "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['gypsum'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:

            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_gypsum_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Imports for consumption"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['gypsum'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_iodine_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[6:10]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]
    elif len(df_data. columns) == 13:
        df_data.columns = ["Production", "unit", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5", "space_6"]


    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['iodine'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_iodine_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Quantity, for consumption", "Exports2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['iodine'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Imports:2":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports2":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['iodine'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_iron_ore_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:25]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Units", "space_1", "year_1",
                           "space_2", "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5"]
    col_to_use = ["Production", "Units"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['ironore'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_iron_ore_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    name = usgs_myb_name(source)
    des = name
    row_to_use = ["Gross weight", "Quantity"]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "Production:":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                product = "imports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data['FlowName'] = "Iron Ore " + product
                data["Description"] = "Iron Ore"
                data["ActivityProducedBy"] = "Iron Ore"
                col_name = usgs_myb_year(YEARS_COVERED['ironore'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_kyanite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[4:13]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "unit",  "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['kyanite'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_kyanite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Quantity2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['kyanite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Exports of kyanite concentrate:3":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, all kyanite minerals:3":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Production:":
                prod = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_lead_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df = pd.read_excel(io.BytesIO(resp.content),
                       sheet_name='T1', header=[3])
    df.columns = df.columns.astype(str).str.strip()
    df = df.rename(columns={df.columns[0]: 'Production',
                            df.columns[1]: 'Units',
                            year: 'FlowAmount'})
    df = df[['Production', 'Units', 'FlowAmount']]

    return df


def usgs_lead_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    name = usgs_myb_name(source)
    des = name
    row_to_use = ["Primary lead, refined content, "
                  "domestic ores and base bullion",
                  "Secondary lead, lead content",
                  "Lead ore and concentrates",
                  "Lead in base bullion",
                  "Lead in base bullion, lead content",
                  "Base bullion"]
    dataframe = pd.DataFrame()
    product = "production"
    for df in df_list:
        for index, row in df.iterrows():
            activity = df.iloc[index]["Production"].strip()
            if activity == "Exports, lead content:":
                product = "exports"
            elif activity == "Imports for consumption, lead content:":
                product = "imports"
            if activity in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["ActivityProducedBy"] = activity
                if str(df.iloc[index]["FlowAmount"]) == "--":
                    data["FlowAmount"] = 0
                else:
                    data["FlowAmount"] = df.iloc[index]["FlowAmount"]
                dataframe = pd.concat([dataframe, pd.DataFrame([data])],
                                      ignore_index=True)
    dataframe = assign_fips_location_system(dataframe, str(year))
    # standardize activityproducedby naming
    dataframe['ActivityProducedBy'] = np.where(
        dataframe['ActivityProducedBy'] == "Base bullion",
        "Lead in base bullion", dataframe['ActivityProducedBy'])

    return dataframe


def usgs_lime_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[16:16]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data_two.loc[28:32]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1.columns) > 12:
        for x in range(12, len(df_data_1.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_1[col_name]
            del df_data_2[col_name]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['lime'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_lime_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Total", "Quantity"]
    import_export = ["Exports:7", "Imports for consumption:7"]
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        prod = "production"
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:7":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:7":
                prod = "imports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['lime'], year)
                data["Description"] = des
                data["ActivityProducedBy"] = name
                if product.strip() == "Total":
                    data['FlowName'] = name + " " + prod
                elif product.strip() == "Quantity":
                    data['FlowName'] = name + " " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_lithium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:8]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_2",  "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['lithium'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_lithium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Exports3", "Imports3", "Production"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['lithium'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports3":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Imports3":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                prod = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_magnesium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:15]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Units", "space_1", "year_1",
                           "space_2", "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['magnesium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_magnesium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Secondary", "Primary", "Exports", "Imports for consumption"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)
    des = name
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Secondary" or \
                    df.iloc[index]["Production"].strip() == "Primary":
                product = "production" + " " + \
                          df.iloc[index]["Production"].strip()
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['magnesium'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_manganese_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:9]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]
    if len(df_data.columns) > 12:
        for x in range(12, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Unit", "space_1", "year_1",
                           "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['manganese'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_manganese_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Exports", "Imports for consumption"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['manganese'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['manganese'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_ma_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T2')
    df_data = pd.DataFrame(df_raw_data.loc[6:7]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 9:
        for x in range(9, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 9:
        df_data.columns = ["Product", "space_1", "quality_year_1", "space_2",
                           "value_year_1", "space_3",
                           "quality_year_2", "space_4", "value_year_2"]
    elif len(df_data. columns) == 9:
        df_data.columns = ["Product", "space_1", "quality_year_1", "space_2",
                           "value_year_1", "space_3",
                           "quality_year_2", "space_4", "value_year_2"]

    col_to_use = ["Product"]
    col_to_use.append("quality_"
                      + usgs_myb_year(YEARS_COVERED['manufacturedabrasive'],
                                      year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_ma_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Silicon carbide"]
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index][
                "Product"].strip().translate(remove_digits)
            if product in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data['FlowName'] = "Silicon carbide"
                data["ActivityProducedBy"] = "Silicon carbide"
                data["Unit"] = "Metric Tons"
                col_name = ("quality_"
                            + usgs_myb_year(
                                YEARS_COVERED['manufacturedabrasive'], year))
                col_name_array = col_name.split("_")
                data["Description"] = product + " " + col_name_array[0]
                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_mica_call(*, resp, source, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[4:6]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]
    name = usgs_myb_name(source)
    des = name
    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "Unit", "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['mica'], year))
    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_mica_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['mica'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Production, sold or used by producers:":
                prod = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_molybdenum_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:11]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['molybdenum'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_molybdenum_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Imports for consumption", "Exports"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)
    des = name

    for df in df_list:
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['molybdenum'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_nickel_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T10')
    df_data_1 = pd.DataFrame(df_raw_data.loc[36:36]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_2 = pd.DataFrame(df_raw_data_two.loc[11:16]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1.columns) > 11:
        for x in range(11, len(df_data_1.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_1[col_name]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    if len(df_data_2.columns) == 12:
        df_data_2.columns = ["Production", "space_1", "space_2", "year_1",
                             "space_3", "year_2", "space_4", "year_3",
                             "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['nickel'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_nickel_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Ores and concentrates3",
                  "United States, sulfide ore, concentrate"]
    import_export = ["Exports:", "Imports for consumption:"]
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        prod = "production"
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                prod = "imports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['nickel'], year)
                if product.strip() == \
                        "United States, sulfide ore, concentrate":
                    data["Description"] = \
                        "United States, sulfide ore, concentrate Nickel"
                    data["ActivityProducedBy"] = name
                    data['FlowName'] = name + " " + prod
                elif product.strip() == "Ores and concentrates":
                    data["Description"] = "Ores and concentrates Nickel"
                    data["ActivityProducedBy"] = name
                    data['FlowName'] = name + " " + prod
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(4)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_niobium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[4:19]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 13:
        for x in range(13, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 13:
        df_data.columns = ["Production", "space_1", "Unit_1", "space_2",
                           "year_1", "space_3", "year_2", "space_4",
                           "year_3", "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['niobium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_niobium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Total imports, Nb content", "Total exports, Nb content"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['niobium'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                product = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['niobium'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_peat_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    """Calls the excel sheet for nickel and removes extra columns"""
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[7:18]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 12:
        for x in range(12, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]

    if len(df_data_one.columns) == 12:
        df_data_one.columns = ["Production", "Unit", "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['peat'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_peat_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Exports", "Imports for consumption"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['peat'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_perlite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:6]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data_one.loc[20:25]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "space_1", "space_2", "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]
        df_data_two.columns = ["Production", "space_1", "space_2", "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['perlite'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_perlite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Mine production2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['perlite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Mine production2":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:3":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports:3":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_phosphate_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    # replace cell in column one and then set row 4 as col names
    df_raw_data_one.iloc[4,0] = 'Production'
    df_raw_data_one.columns = df_raw_data_one.iloc[4]
    df_raw_data_one.columns = df_raw_data_one.columns.astype(str).str.replace(
        ".0", '')

    df_data_one = pd.DataFrame(df_raw_data_one.loc[7:9]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data_one.loc[19:21]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    col_to_use = ["Production", year]
    df_data_one = df_data_one[col_to_use]
    df_data_two = df_data_two[col_to_use]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_phosphate_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Gross weight", "Quantity, gross weight"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = year
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Marketable production:":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:3":
                prod = "import"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_platinum_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data.loc[4:9]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[18:30]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 13:
        df_data_1.columns = ["Production", "space_6", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
        df_data_2.columns = ["Production", "space_6", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
    elif len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
        df_data_2.columns = ["Production", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['platinum'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
            del df_data_2[col]

    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_platinum_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Palladium, Pd content",
                  "Platinum, includes coins, Pt content",
                  "Platinum, Pt content",
                  "Iridium, Ir content", "Osmium, Os content",
                  "Rhodium, Rh content", "Ruthenium, Ru content",
                  "Iridium, osmium, and ruthenium, gross weight",
                  "Rhodium, Rh content"]
    dataframe = pd.DataFrame()

    for df in df_list:
        previous_name = ""
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "Exports, refined:":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, refined:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Mine production:2":
                product = "production"

            name_array = df.iloc[index]["Production"].strip().split(",")

            if product == "production":
                name_array = previous_name.split(",")

            previous_name = df.iloc[index]["Production"].strip()
            name = name_array[0]

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "kilograms"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['platinum'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_potash_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    # replace cell in column one and then set row 4 as col names
    df_raw_data_one.iloc[4, 0] = 'Production'
    df_raw_data_one.columns = df_raw_data_one.iloc[4]
    df_raw_data_one.columns = df_raw_data_one.columns.astype(str).str.replace(
        ".0", '')

    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:8]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data_one.loc[17:23]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]



    col_to_use = ["Production", year]
    df_data_one = df_data_one[col_to_use]
    df_data_two = df_data_two[col_to_use]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_potash_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["K2O equivalent"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = year
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production:3":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:6":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_pumice_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:11]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 13:
        for x in range(13, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]

    if len(df_data_one. columns) == 13:
        df_data_one.columns = ["Production", "space_1", "Unit", "space_2",
                               "year_1", "space_3", "year_2", "space_4",
                               "year_3", "space_5", "year_4", "space_6",
                               "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['pumice'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_pumice_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Imports for consumption3", "Exports3"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['pumice'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption3":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports3":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_rhenium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[5:13]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 14:
        for x in range(14, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 14:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5", "space_6",
                           "space_7", "space_8"]
    elif len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['rhenium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]
    return df_data


def usgs_rhenium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Total, rhenium content",
                  "Production, mine, rhenium content2"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)
    des = name
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Total, rhenium content":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Production, mine, rhenium content2":
                product = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "kilograms"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['rhenium'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_salt_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:11]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data_one.loc[15:19]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]
            del df_data_two[col_name]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1",  "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['salt'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_salt_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Total"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['salt'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production:2":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_sgc_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[5:12]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['sandgravelconstruction'],
                                    year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]

    return df_data_1


def usgs_sgc_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    dataframe = pd.DataFrame()
    for df in df_list:

        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Sold or used by producers:2":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(
                    YEARS_COVERED['sandgravelconstruction'], year)
                data["Description"] = "Sand Gravel Construction"
                data["ActivityProducedBy"] = "Sand Gravel Construction"
                if product.strip() == "Quantity":
                    data['FlowName'] = "Sand Gravel Construction " + prod
                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_sgi_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[6:10]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data_two.loc[15:19]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1.columns) > 12:
        for x in range(12, len(df_data_1.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_1[col_name]
            del df_data_2[col_name]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "space_7", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns = ["Production", "space_7", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['sandgravelindustrial'],
                                    year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
            del df_data_2[col]

    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_sgi_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Total"]
    dataframe = pd.DataFrame()
    for df in df_list:

        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Sold or used:":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['sandgravelindustrial'],
                                         year)
                data["Description"] = "Sand Gravel Industrial"
                data["ActivityProducedBy"] = "Sand Gravel Industrial"
                data['FlowName'] = "Sand Gravel Industrial " + prod
                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_silver_call(*, resp, year, **_):
    """
     Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[4:14]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "Unit", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['silver'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]
    return df_data


def usgs_silver_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Ore and concentrate", "Ore and concentrate2", "Quantity"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)
    des = name
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Ore and concentrate2":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Quantity":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Ore and concentrate":
                product = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['silver'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def description(value, code):
    """
    Create string for column based on row description
    :param value: str, description column for a row
    :param code: str, NAICS code
    :return: str, to use as column value
    """
    glass_list = ["Container", "Flat", "Fiber", "Other", "Total"]
    other_list = ["Total domestic consumption4"]
    export_list = ["Canada"]
    return_val = ""
    if value in glass_list:
        return_val = "Glass " + value
        if math.isnan(code):
            return_val = value
        if value == "Total":
            return_val = "Glass " + value
    elif value in other_list:
        return_val = "Other " + value
    elif value in export_list:
        return_val = "Exports " + value
    else:
        return_val = value
    return_val = usgs_myb_remove_digits(return_val)
    return return_val


def soda_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    col_to_use = ["Production", "NAICS code", "End use", "year_5", "total"]
    years_covered = YEARS_COVERED['sodaash_t4']
    if str(year) in years_covered:
        df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T4')
        df_data_one = pd.DataFrame(df_raw_data.loc[7:25]).reindex()
        df_data_one = df_data_one.reset_index()
        del df_data_one["index"]
        if len(df_data_one.columns) == 23:
            df_data_one.columns = ["NAICS code", "space_1", "Production",
                                   "space_2", "y1_q1", "space_3", "y1_q2",
                                   "space_4", "y1_q3", "space_5", "y1_q4",
                                   "space_6", "year_4", "space_7", "y2_q1",
                                   "space_8", "y2_q2", "space_9", "y2_q3",
                                   "space_10", "y2_q4", "space_11", "year_5"]
        elif len(df_data_one.columns) == 17:
            df_data_one.columns = ["NAICS code", "space_1", "Production",
                                   "space_2", "last_year", "space_3", "y1_q1",
                                   "space_4", "y1_q2", "space_5", "y1_q3",
                                   "space_6", "y1_4", "space_7", "year_5",
                                   "space_8", "space_9"]

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_two = pd.DataFrame(df_raw_data_two.loc[6:18]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_two.columns) == 11:
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]

    if str(year) in years_covered:
        for col in df_data_one.columns:
            if col not in col_to_use:
                del df_data_one[col]

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]

    if str(year) in years_covered:
        frames = [df_data_one, df_data_two]
    else:
        frames = [df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def soda_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    total_glass = 0

    data = []
    row_to_use = ["Quantity", "Quantity2"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    col_name = "year_5"
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            data = usgs_myb_static_variables()
            data["Unit"] = "Thousand metric tons"
            data["FlowAmount"] = str(df.iloc[index][col_name])
            data["SourceName"] = source
            data["Year"] = str(year)
            data['FlowName'] = name

            if str(df.iloc[index]["Production"]) != "nan":
                des = name
                if df.iloc[index]["Production"].strip() == "Exports:":
                    prod = "exports"
                elif df.iloc[index]["Production"].strip() == \
                        "Imports for consumption:":
                    prod = "imports"
                elif df.iloc[index]["Production"].strip() == "Production:":
                    prod = "production"
                if df.iloc[index]["Production"].strip() in row_to_use:
                    product = df.iloc[index]["Production"].strip()
                    data["SourceName"] = source
                    data["Year"] = str(year)
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                    if str(df.iloc[index][col_name]) == "W":
                        data["FlowAmount"] = WITHDRAWN_KEYWORD
                    data["Description"] = des
                    data["ActivityProducedBy"] = name
                    data['FlowName'] = name + " " + prod
                    dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                    dataframe = assign_fips_location_system(
                        dataframe, str(year))
            else:
                data["Class"] = "Chemicals"
                data["Context"] = None
                data["Compartment"] = "air"
                data["Description"] = ""
                data['ActivityConsumedBy'] = \
                    description(df.iloc[index]["End use"],
                                df.iloc[index]["NAICS code"])
                data['FlowName'] = (
                    name + " " + description(df.iloc[index]["End use"],
                                             df.iloc[index]["NAICS code"])
                )
                if df.iloc[index]["End use"].strip() == "Glass:":
                    total_glass = int(df.iloc[index]["NAICS code"])
                elif data['ActivityConsumedBy'] == "Glass Total":
                    data["Description"] = total_glass

                if not math.isnan(df.iloc[index][col_name]):
                    data["FlowAmount"] = int(df.iloc[index][col_name])
                    data["ActivityProducedBy"] = None
                    if not math.isnan(df.iloc[index]["NAICS code"]):
                        des_str = str(df.iloc[index]["NAICS code"])
                        data["Description"] = des_str
                if df.iloc[index]["End use"].strip() != "Glass:":
                    dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                    dataframe = assign_fips_location_system(
                        dataframe, str(year))
    return dataframe


def usgs_stonecr_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[5:15]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    if len(df_data_1.columns) > 11:
        for x in range(11, len(df_data_1.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_1[col_name]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['stonecrushed'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]

    return df_data_1


def usgs_stonecr_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity"]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Sold or used by producers:2":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:3":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Recycle:":
                prod = "recycle"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['stonecrushed'], year)
                data["Description"] = "Stone Crushed"
                data["ActivityProducedBy"] = "Stone Crushed"
                data['FlowName'] = "Stone Crushed " + prod
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if prod != "recycle":
                    dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                    dataframe = assign_fips_location_system(
                        dataframe, str(year))
    return dataframe


def usgs_stonedis_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[6:9]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['stonedimension'], year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]

    return df_data_1


def usgs_stonedis_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", ]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, value":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports, value":
                prod = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['stonedimension'], year)
                data["Description"] = "Stone Dimension"
                data["ActivityProducedBy"] = "Stone Dimension"
                data['FlowName'] = "Stone Dimension " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_strontium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[6:13]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data.columns) > 11:
        for x in range(11, len(df_data.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data[col_name]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['strontium'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_strontium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production, strontium minerals", "Strontium compounds3",
                  "Celestite4", "Strontium carbonate"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['strontium'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:2":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == \
                    "Production, strontium minerals":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports:2":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                if usgs_myb_remove_digits(
                        df.iloc[index]["Production"].strip()) == "Celestite":
                    data['FlowName'] = \
                        name + " " + product + " " + usgs_myb_remove_digits(
                            df.iloc[index]["Production"].strip())
                else:
                    data['FlowName'] = name + " " + product
                data["Description"] = usgs_myb_remove_digits(
                    df.iloc[index]["Production"].strip())
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['strontium'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_talc_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:8]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data_one.loc[20:25]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_3",
                               "year_2", "space_4", "year_3", "space_5",
                               "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['talc'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_talc_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Talc"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['talc'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Mine production, crude:":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, talc:2":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exports, talc:2":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_titanium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data.loc[4:7]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[12:15]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 13:
        df_data_1.columns = ["Production", "space_1", "Unit", "space_6",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
        df_data_2.columns = ["Production", "space_1", "Unit", "space_6",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['titanium'], year))
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
            del df_data_1[col]

    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_titanium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production2", "Production", "Imports for consumption"]
    dataframe = pd.DataFrame()
    name = ""

    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production2" or \
                    df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            if df.iloc[index]["Production"].strip() == "Mineral concentrates:":
                name = "Titanium"
            elif df.iloc[index]["Production"].strip() == \
                    "Titanium dioxide pigment:":
                name = "Titanium dioxide"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['titanium'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_tungsten_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:10]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2",
                           "year_2", "space_3", "year_3", "space_4",
                           "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['tungsten'], year))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_tungsten_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Exports", "Imports for consumption"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['tungsten'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['tungsten'], year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "nan":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_vermiculite_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[6:12]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "Unit", "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]
    elif len(df_data_one. columns) == 13:
        df_data_one.columns = ["Production", "Unit", "space_2", "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5", "space_7"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['vermiculite'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_vermiculite_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production, concentratee, 2, 3", "Exportse, 4",
                  "Imports for consumptione, 4"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['vermiculite'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Production, concentratee, 2, 3":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumptione, 4":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exportse, 4":
                prod = "export"
            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                data["FlowAmount"] = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_zeolites_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[4:7]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "Unit", "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['zeolites'], year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_zeolites_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Exportse", "Importse"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(YEARS_COVERED['zeolites'], year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == "Importse":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exportse":
                prod = "export"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                flow_amount = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                flow_amount = flow_amount.replace("<", "")
                flow_amount = flow_amount.replace(",", "")
                data["FlowAmount"] = flow_amount
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_zinc_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_two = pd.DataFrame(df_raw_data_two.loc[9:20]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T9')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[53:53]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]

    if len(df_data_two. columns) == 12:
        df_data_two.columns = ["Production",  "unit", "space_1", "year_1",
                               "space_2", "year_2", "space_3", "year_3",
                               "space_4", "year_4", "space_5", "year_5"]
    if len(df_data_one.columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['zinc'], year))

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_zinc_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Ores and concentrates, zinc content",
                  "United States"]
    import_export = ["Exports:", "Imports for consumption:",
                     "Recoverable zinc:"]
    prod = ""
    name = usgs_myb_name(source)
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption:":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Recoverable zinc:":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == "United States":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(YEARS_COVERED['zinc'], year)
                data["FlowAmount"] = str(df.iloc[index][col_name])

                if product.strip() == "Quantity":
                    data["Description"] = "zinc in concentrate"
                    data["ActivityProducedBy"] = "zinc in concentrate "
                    data['FlowName'] = "zinc in concentrate " + prod
                elif product.strip() == "Ores and concentrates, zinc content":
                    data["Description"] = "Ores and concentrates, zinc content"
                    data["ActivityProducedBy"] = \
                        "Ores and concentrates, zinc content"
                    data['FlowName'] = \
                        "Ores and concentrates, zinc content " + prod
                elif product.strip() == "United States":
                    data["Description"] = "Zinc; Mine"
                    data["ActivityProducedBy"] = name + " " + prod
                    data['FlowName'] = "Zinc; Mine"

                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe


def usgs_zirconium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data.loc[6:10]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data.loc[24:24]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]
            del df_data_two[col_name]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3",
                               "space_4", "year_4", "space_5", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3",
                               "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(YEARS_COVERED['zirconium'], year))
    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_zirconium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Imports for consumption3", "Concentrates", "Exports",
                  "Hafnium, unwrought, including powder, "
                  "imports for consumption"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)

    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption3":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Concentrates":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"

            if df.iloc[index]["Production"].strip() == \
                    "Hafnium, unwrought, including powder, imports for " \
                    "consumption":
                prod = "imports"
                des = df.iloc[index]["Production"].strip()
            else:
                des = name

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_variables()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(YEARS_COVERED['zirconium'], year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = pd.concat([dataframe, pd.DataFrame.from_dict([data])], ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
