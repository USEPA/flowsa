# EPA_GHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks
"""

import io
import zipfile
import numpy as np
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.dataclean import replace_NoneType_with_empty_cells
from flowsa.settings import log, externaldatapath
from flowsa.schema import flow_by_activity_fields
from flowsa.common import load_yaml_dict
from flowsa.data_source_scripts import EIA_MECS


SECTOR_DICT = {'Res.': 'Residential',
               'Comm.': 'Commercial',
               'Ind.': 'Industrial',
               'Trans.': 'Transportation',
               'Elec.': 'Electricity Power',
               'Terr.': 'U.S. Territory'}

ANNEX_HEADERS = {"Total Consumption (TBtu) a": "Total Consumption (TBtu)",
                 "Adjustments (TBtu) b": "Adjustments (TBtu)",
                 "Adjusted Consumption (TBtu) a": "Adjusted Consumption (TBtu)",
                 "Emissions b (MMT CO2 Eq.) from Energy Use":
                     "Emissions (MMT CO2 Eq.) from Energy Use"
                 }

# Tables for annual CO2 emissions from fossil fuel combustion
ANNEX_ENERGY_TABLES = ["A-10", "A-11", "A-12", "A-13", "A-14", "A-15", "A-16",
                       "A-17", "A-18", "A-19", "A-20"]

SPECIAL_FORMAT = ["3-10", "3-22", "3-22b", "4-46", "5-29",
                  "A-93", "A-94", "A-118", ]


DROP_COLS = ["Unnamed: 0"] + list(pd.date_range(
    start="1990", end="2010", freq='Y').year.astype(str))

YEARS = list(pd.date_range(start="2010", end="2020", freq='Y').year.astype(str))


def ghg_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    annex_url = config['url']['annex_url']
    return [build_url, annex_url]


def cell_get_name(value, default_flow_name):
    """
    Given a single string value (cell), separate the name and units.
    :param value: str
    :param default_flow_name: indicate return flow name string subset
    :return: flow name for row
    """
    if '(' not in value:
        return default_flow_name.replace('__type__', value.strip())

    spl = value.split(' ')
    name = ''
    found_units = False
    for sub in spl:
        if '(' not in sub and not found_units:
            name = f'{name.strip()} {sub}'
        else:
            found_units = True
    return name.strip()


def cell_get_units(value, default_units):
    """
    Given a single string value (cell), separate the name and units.
    :param value: str
    :param default_units: indicate return units string subset
    :return: unit for row
    """
    if '(' not in value:
        return default_units

    spl = value.split(' ')
    name = ''
    found_units = False
    for sub in spl:
        if ')' in sub:
            found_units = False
        if '(' in sub or found_units:
            name = f'{name} {sub.replace("(", "").replace(")", "")} '
            found_units = True
    return name.strip()


def series_separate_name_and_units(series, default_flow_name, default_units):
    """
    Given a series (such as a df column), split the contents' strings into a name and units.
    An example might be converting "Carbon Stored (MMT C)" into ["Carbon Stored", "MMT C"].

    :param series: df column
    :param default_flow_name: df column for flow name to be modified
    :param default_units: df column for units to be modified
    :return: str, flowname and units for each row in df
    """
    names = series.apply(lambda x: cell_get_name(x, default_flow_name))
    units = series.apply(lambda x: cell_get_units(x, default_units))
    return {'names': names, 'units': units}


def annex_yearly_tables(data, table=None):
    """Special handling of ANNEX Energy Tables"""
    df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                     header=[0, 1], thousands=",")
    if table == "A-10":
        # Extra row to drop in this table
        df = df.drop([0])
    header_name = ""
    newcols = []  # empty list to have new column names
    for i in range(len(df.columns)):
        fuel_type = str(df.iloc[0, i])
        for abbrev, full_name in SECTOR_DICT.items():
            fuel_type = fuel_type.replace(abbrev, full_name)
        fuel_type = fuel_type.strip()

        col_name = df.columns[i][1]
        if "Unnamed" in col_name:
            column_name = header_name
        elif col_name in ANNEX_HEADERS.keys():
            column_name = ANNEX_HEADERS[col_name]
            header_name = ANNEX_HEADERS[col_name]

        newcols.append(f"{column_name} - {fuel_type}")
    df.columns = newcols  # assign column names
    df = df.iloc[1:, :]  # exclude first row
    df.dropna(how='all', inplace=True)
    df = df.reset_index(drop=True)
    return df


def ghg_call(*, resp, url, year, config, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param url: string, url
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: pandas dataframe of original source data
    """
    df = None
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        frames = []
        if 'annex' in url:
            is_annex = True
            t_tables = config['Annex']
        else:
            is_annex = False
            t_tables = config['Tables']
        for chapter, tables in t_tables.items():
            for table in tables:
                tbl_year = tables[table].get('year')
                if tbl_year is not None and tbl_year != year:
                    # Skip tables when the year does not align with target year
                    continue

                if is_annex:
                    path = f"Annex/Table {table}.csv"
                else:
                    path = f"Chapter Text/{chapter}/Table {table}.csv"

                # Handle special case of table 3-22 in external data folder
                if table == "3-22b":
                    if str(year) == '2019':
                        # Skip 3-22b for year 2019 (use 3-22 instead)
                        continue
                    else:
                        df = pd.read_csv(f"{externaldatapath}/GHGI_Table_{table}.csv",
                                         skiprows=2, encoding="ISO-8859-1", thousands=",")
                else:
                    data = f.open(path)

                if table not in SPECIAL_FORMAT + ANNEX_ENERGY_TABLES:
                    # Default case
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                     thousands=",")
                elif table in ['3-10', '4-46', '5-29',
                               'A-93', 'A-94', 'A-118']:
                    # Skip single row
                    df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                     thousands=",", decimal=".")
                elif table == "3-22":
                    # Skip first two rows, as usual, but make headers the next 3 rows:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                     header=[0, 1, 2], thousands=",")
                    # The next two rows are headers and the third is units:
                    new_headers = []
                    for col in df.columns:
                        # unit = col[2]
                        new_header = 'Unnamed: 0'
                        if 'Unnamed' not in col[0]:
                            if 'Unnamed' not in col[1]:
                                new_header = f'{col[0]} {col[1]}'
                            else:
                                new_header = col[0]
                            if 'Unnamed' not in col[2]:
                                new_header += f' {col[2]}'
                            # unit = col[2]
                        elif 'Unnamed' in col[0] and 'Unnamed' not in col[2]:
                            new_header = col[2]
                        new_headers.append(new_header)
                    df.columns = new_headers
                elif table in ANNEX_ENERGY_TABLES:
                    df = annex_yearly_tables(data, table)

                if df is not None and len(df.columns) > 1:
                    years = YEARS.copy()
                    years.remove(str(year))
                    df = df.drop(columns=(DROP_COLS + years), errors='ignore')
                    df["SourceName"] = f"EPA_GHGI_T_{table.replace('-', '_')}"
                    frames.append(df)
                else:
                    log.warning(f"Error in generating {table}")
        return frames


def get_unnamed_cols(df):
    """
    Get a list of all unnamed columns, used to drop them.
    :param df: df being formatted
    :return: list, unnamed columns
    """
    return [col for col in df.columns if "Unnamed" in col]


def get_table_meta(source_name, config):
    """Find and return table meta from source_name."""
    if "_A_" in source_name:
        td = config['Annex']
    else:
        td = config['Tables']
    for chapter in td.keys():
        for k, v in td[chapter].items():
            if source_name.endswith(k.replace("-", "_")):
                return v

def is_consumption(source_name, config):
    """
    Determine whether the given source contains consumption or production data.
    :param source_name: df
    :return: True or False
    """
    if 'consum' in get_table_meta(source_name, config)['desc'].lower():
        return True
    return False

def strip_char(text):
    """
    Removes the footnote chars from the text
    """
    text = text + " "
    notes = [" a ", " b ", " c ", " d ", " e ", " f ", " g ",
             " h ", " i ", " j ", " k ", " b,c ", " h,i ", " f,g "]
    for i in notes:
        if i in text:
            text_split = text.split(i)
            text = text_split[0]
    return text.strip()


def ghg_parse(*, df_list, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    cleaned_list = []
    for df in df_list:
        source_name = df["SourceName"][0]
        table_name = source_name[11:].replace("_","-")
        log.info(f'Processing {source_name}')

        # Specify to ignore errors in case one of the drop_cols is missing.
        df = df.drop(columns=get_unnamed_cols(df), errors='ignore')
        is_cons = is_consumption(source_name, config)

        # Rename to "ActivityProducedBy" or "ActivityConsumedBy":
        if is_cons:
            df = df.rename(columns={df.columns[0]: "ActivityConsumedBy"})
            df["ActivityProducedBy"] = 'None'
        else:
            df = df.rename(columns={df.columns[0]: "ActivityProducedBy"})
            df["ActivityConsumedBy"] = 'None'

        df["FlowType"] = "ELEMENTARY_FLOW"
        df["Location"] = "00000"

        id_vars = ["SourceName", "ActivityConsumedBy", "ActivityProducedBy",
                   "FlowType", "Location"]

        df.set_index(id_vars)

        meta = get_table_meta(source_name, config)

        if table_name in ['3-22']:
            df = df.melt(id_vars=id_vars,
                         var_name=meta.get('melt_var'),
                         value_name="FlowAmount")
            df = df.rename(columns={"ActivityConsumedBy": "ActivityProducedBy",
                                    "ActivityProducedBy": "ActivityConsumedBy"})
            name_unit = series_separate_name_and_units(df['FlowName'],
                                                       meta['activity'],
                                                       meta['unit'])
            df['FlowName'] = name_unit['names']
            df['Unit'] = name_unit['units']
            df['Year'] = year

        elif table_name in ['4-14', '4-33', '4-50', '4-80']:
            # When Year is the first column in the table, need to make this correction
            df = df.rename(columns={'ActivityProducedBy': 'Year',
                                    'Year': 'ActivityProducedBy'})
            # Melt on custom defined variable
            melt_var = meta.get('melt_var')
            if melt_var in id_vars:
                id_vars.remove(melt_var)
            elif 'ActivityProducedBy' not in df:
                df["ActivityProducedBy"] = 'None'
            id_vars.append('Year')
            df = df.melt(id_vars=id_vars, var_name=melt_var,
                         value_name="FlowAmount")

        elif table_name in ANNEX_ENERGY_TABLES:
            df = df.melt(id_vars=id_vars, var_name="FlowName",
                         value_name="FlowAmount")
            df["Year"] = year
            for index, row in df.iterrows():
                col_name = row['FlowName']
                acb = row['ActivityConsumedBy'].strip()
                name_split = col_name.split(" (")
                source = name_split[1].split('- ')[1]
                # Append column name after dash to activity
                activity = f"{acb.strip()} {name_split[1].split('- ')[1]}"

                df.loc[index, 'Description'] = meta['desc']
                if name_split[0] == "Emissions":
                    df.loc[index, 'FlowName'] = meta['emission']
                    df.loc[index, 'Unit'] = meta['emission_unit']
                    df.loc[index, 'Class'] = meta['emission_class']
                    df.loc[index, 'Compartment'] = meta['emission_compartment']
                    df.loc[index, 'ActivityProducedBy'] = activity
                    df.loc[index, 'ActivityConsumedBy'] = "None"
                else: # "Consumption"
                    df.loc[index, 'FlowName'] = acb
                    df.loc[index, 'FlowType'] = "TECHNOSPHERE_FLOW"
                    df.loc[index, 'Unit'] = meta['unit']
                    df.loc[index, 'Class'] = meta['class']
                    df.loc[index, 'ActivityProducedBy'] = "None"
                    df.loc[index, 'ActivityConsumedBy'] = source

        else:
            # Standard years (one or more) as column headers
            df = df.melt(id_vars=id_vars, var_name="Year",
                         value_name="FlowAmount")


        # Dropping all rows with value "+": represents non-zero value
        df["FlowAmount"].replace("\+", np.nan, inplace=True, regex=True)
        # Dropping all rows with value "NE"
        df["FlowAmount"].replace("NE", np.nan, inplace=True)
        # Convert all empty cells to nan cells
        df["FlowAmount"].replace("", np.nan, inplace=True)
        # Table 3-10 has some NO (Not Occuring) values, dropping these.
        df["FlowAmount"].replace("NO", np.nan, inplace=True)
        # Table A-118 has some IE values, dropping these.
        df["FlowAmount"].replace("IE", np.nan, inplace=True)
        df["FlowAmount"].replace(r'NO ', np.nan, inplace=True)

        # Drop any nan rows
        df.dropna(subset=['FlowAmount'], inplace=True)

        if table_name not in ANNEX_ENERGY_TABLES:
            if 'Unit' not in df:
                df.loc[df["SourceName"] == source_name, "Unit"] = meta.get("unit")
            if 'FlowName' not in df:
                df.loc[df["SourceName"] == source_name, "FlowName"] = meta.get("flow")

            df.loc[df["SourceName"] == source_name, "Class"] = meta.get("class")
            df.loc[df["SourceName"] == source_name, "Description"] = meta.get("desc")
            df.loc[df["SourceName"] == source_name, "Compartment"] = meta.get("compartment")

        if 'Year' not in df.columns:
            df['Year'] = year
        else:
            df = df[df['Year'].astype(str).isin([year])]

        # Add DQ scores
        df["DataReliability"] = 5  # tmp
        df["DataCollection"] = 5  # tmp
        # Fill in the rest of the Flow by fields so they show "None" instead of nan
        df["MeasureofSpread"] = 'None'
        df["DistributionType"] = 'None'
        df["LocationSystem"] = 'None'
        df = assign_fips_location_system(df, str(year))

        # modified_activity_list = ["ES-5"]
        multi_chem_names = ["2-1", "4-46", "5-7", "5-29", "ES-5"]
        source_No_activity = ["3-22", "3-22b"]
        # Handle tables with 1 parent level category
        source_activity_1 = ["3-7", "3-8", "3-9", "3-10", "3-14", "3-15",
                             "5-18", "5-19", "A-76", "A-77"]
        # Tables with sub categories
        source_activity_2 =  ["3-38", "3-63", "A-103"]

        if table_name in multi_chem_names:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["CO2", "CH4", "N2O", "NF3", "HFCs", "PFCs",
                              "SF6", "NF3", "CH4 a", "N2O b", "CO", "NOx"]
            for index, row in df.iterrows():
                apb_value = row["ActivityProducedBy"]
                if "CH4" in apb_value:
                    apb_value = "CH4"
                elif "N2O" in apb_value and apb_value != "N2O from Product Uses":
                    apb_value = "N2O"
                elif "CO2" in apb_value:
                    apb_value = "CO2"

                if apb_value in flow_name_list:
                    apbe_value = apb_value
                    df.loc[index, 'FlowName'] = apbe_value
                    df.loc[index, 'ActivityProducedBy'] = "All activities"
                    bool_apb = True
                elif apb_value.startswith('LULUCF'):
                    df.loc[index, 'FlowName'] = 'CO2e'
                    df.loc[index, 'ActivityProducedBy'] = strip_char(apb_value)
                elif apb_value.startswith('Total'):
                    df = df.drop(index)
                else:
                    apb_txt = df.loc[index, 'ActivityProducedBy']
                    apb_txt = strip_char(apb_txt)
                    df.loc[index, 'ActivityProducedBy'] = apb_txt
                    if bool_apb == True:
                        df.loc[index, 'FlowName'] = apbe_value

        elif table_name in source_No_activity:
            apbe_value = ""
            flow_name_list = ["Industry", "Transportation", "U.S. Territories"]
            for index, row in df.iterrows():
                unit = row["Unit"]
                if unit.strip() == "MMT  CO2":
                        df.loc[index, 'Unit'] = "MMT CO2e"
                if df.loc[index, 'Unit'] != "MMT CO2e":
                    df = df.drop(index)
                else:
                    df.loc[index, 'FlowName'] = meta.get('flow')
                    # use .join and split to remove interior spaces
                    apb_value = " ".join(row["ActivityProducedBy"].split())
                    apb_value = apb_value.replace("°", "")
                    if apb_value in flow_name_list:
                        # set header
                        apbe_value = apb_value
                        df.loc[index, 'ActivityProducedBy'
                               ] = f"{apbe_value} All activities"
                    else:
                        # apply header
                        apb_txt = strip_char(apb_value)
                        df.loc[index, 'ActivityProducedBy'
                               ] = f"{apbe_value} {apb_txt}"
                    if "Total" == apb_value or "Total " == apb_value:
                        df = df.drop(index)

        elif table_name in source_activity_1:
            apbe_value = ""
            activity_subtotal = ["Electric Power", "Industrial", "Commercial",
                                 "Residential", "U.S. Territories",
                                 "Transportation",
                                 "Fuel Type/Vehicle Type", "Diesel On-Road",
                                 "Alternative Fuel On-Road", "Non-Road",
                                 "Gasoline On-Road", "Exploration",
                                 "Production (Total)", "Refining",
                                 "Crude Oil Transportation",
                                 "Cropland", "Grassland"]
            for index, row in df.iterrows():
                apb_value = strip_char(row["ActivityProducedBy"])
                if apb_value in activity_subtotal:
                    # set the header
                    apbe_value = apb_value
                    df.loc[index, 'ActivityProducedBy'
                           ] = f"All activities {apbe_value}"
                else:
                    # apply the header
                    apb_txt = apb_value
                    if table_name == "3-10":
                        # Separate Flows and activities for this table
                        df.loc[index, 'ActivityProducedBy'] = apbe_value
                        df.loc[index, 'FlowName'] = apb_txt
                    else:
                        df.loc[index, 'ActivityProducedBy'
                               ] = f"{apb_txt} {apbe_value}"
                if apb_value.startswith("Total"):
                    df = df.drop(index)

        elif table_name in source_activity_2:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["Explorationb", "Production", "Processing",
                              "Transmission and Storage", "Distribution",
                              "Crude Oil Transportation", "Refining",
                              "Exploration", "Mobile AC",
                              "Refrigerated Transport",
                              "Comfort Cooling for Trains and Buses"]
            for index, row in df.iterrows():
                apb_value = row["ActivityProducedBy"]
                start_activity = row["FlowName"]
                if apb_value.strip() in flow_name_list:
                    apbe_value = apb_value
                    if apbe_value == "Explorationb":
                        apbe_value = "Exploration"
                    df.loc[index, 'FlowName'] = start_activity
                    df.loc[index, 'ActivityProducedBy'] = apbe_value
                    bool_apb = True
                else:
                    if bool_apb == True:
                        df.loc[index, 'FlowName'] = start_activity
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        if apb_txt == "Gathering and Boostingc":
                            apb_txt = "Gathering and Boosting"
                        df.loc[index, 'ActivityProducedBy'
                               ] = f"{apbe_value} - {apb_txt}"
                    else:
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'
                               ] = f"{apb_txt} {apbe_value}"
                if "Total" == apb_value or "Total " == apb_value:
                    df = df.drop(index)

        elif table_name == "A-79":
            fuel_name = ""
            A_79_unit_dict = {'Natural Gas': 'trillion cubic feet',
                              'Electricity': 'million kilowatt-hours'}
            df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
            for index, row in df.iterrows():
                if row["ActivityConsumedBy"].startswith(' '):
                    # indicates subcategory
                    df.loc[index, 'ActivityConsumedBy'] = strip_char(
                        df.loc[index, 'ActivityConsumedBy'])
                    df.loc[index, 'FlowName'] = fuel_name
                else:
                    # fuel header
                    fuel_name = df.loc[index, 'ActivityConsumedBy']
                    fuel_name = strip_char(fuel_name)
                    df.loc[index, 'ActivityConsumedBy'] = "All activities"
                    df.loc[index, 'FlowName'] = fuel_name
                if fuel_name in A_79_unit_dict.keys():
                    df.loc[index, 'Unit'] = A_79_unit_dict[fuel_name]

        else:
            if table_name in ["4-48"]:
                # Assign activity as flow for technosphere flows
                df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']

            elif table_name in ["4-84", "4-94", "4-99"]:
                # Table with flow names as Rows
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')

            elif table_name in ["4-33", "4-50", "4-80"]:
                # Table with units or flows as columns
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')
                df.loc[df['Unit'] == 'MMT CO2 Eq.', 'Unit'] = 'MMT CO2e'
                df.loc[df['Unit'].str.contains('kt'), 'Unit'] = 'kt'

            elif table_name in ["4-14", "4-99"]:
                # Remove notes from activity names
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    if "(" in apb_value:
                        text_split = apb_value.split("(")
                        df.loc[index, 'ActivityProducedBy'] = text_split[0]

            elif table_name in ["A-101"]:
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    df.loc[index, 'ActivityProducedBy'] = apb_value

            else:
                for index, row in df.iterrows():
                    if "CO2" in row['Unit']:
                        if table_name in ["3-42", "3-67"]:
                            df.loc[index, 'Unit'] = df.loc[index, 'Unit']
                        else:
                            df.loc[index, 'Unit'] = "MMT CO2e"

        df['ActivityProducedBy'] = df['ActivityProducedBy'].str.strip()
        df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
        df['FlowName'] = df['FlowName'].str.strip()

        # Update location for terriory-based activities
        df.loc[(df['ActivityProducedBy'].str.contains("U.S. Territor")) |
               (df['ActivityConsumedBy'].str.contains("U.S. Territor")),
               'Location'] = "99000"

        df.drop(df.loc[df['ActivityProducedBy'] == "Total"].index, inplace=True)
        df.drop(df.loc[df['FlowName'] == "Total"].index, inplace=True)

        df = df.loc[:, ~df.columns.duplicated()]
        # Remove commas from numbers again in case any were missed:
        df["FlowAmount"].replace(',', '', regex=True, inplace=True)
        cleaned_list.append(df)

    return cleaned_list


def get_manufacturing_energy_ratios(year):
    """Calculate energy ratio by fuel between GHGI and EIA MECS."""
    # flow correspondence between GHGI and MECS
    flow_corr = {'Industrial Other Coal': 'Coal',
                 'Natural Gas': 'Natural Gas',
                 }

    def closest_value(input_list, input_value):
        difference = lambda input_list : abs(input_list - input_value)
        return min(input_list, key=difference)

    mecs_year = closest_value(load_yaml_dict('EIA_MECS_Energy',
                                             flowbytype='FBA').get('years'),
                              year)

    # Filter MECS for total national energy consumption for manufacturing sectors
    mecs = load_fba_w_standardized_units(datasource='EIA_MECS_Energy',
                                         year=mecs_year,
                                         flowclass='Energy')
    mecs = mecs.loc[(mecs['ActivityConsumedBy'] == '31-33') &
                    (mecs['Location'] == '00000')].reset_index(drop=True)
    mecs = EIA_MECS.mecs_energy_fba_cleanup(mecs, None)

    # Identify the GHGI table that matches EIA_MECS
    for t, v in (load_yaml_dict('EPA_GHGI', 'FBA')
                 .get('Annex').get('Annex').items()):
        if ((v.get('class') == 'Energy')
        & ('Energy Consumption Data' in v.get('desc'))
        & (v.get('year') == str(mecs_year))):
                table = f"EPA_GHGI_T_{t.replace('-', '_')}"
                break
    else:
        log.error('unable to identify corresponding GHGI table')

    ghgi = load_fba_w_standardized_units(datasource=table,
                                         year=mecs_year,
                                         flowclass='Energy')
    ghgi = ghgi[ghgi['ActivityConsumedBy']=='Industrial'].reset_index(drop=True)

    pct_dict = {}
    for ghgi_flow, mecs_flow in flow_corr.items():
        # Calculate percent energy contribution from MECS based on v
        mecs_energy = mecs.loc[mecs['FlowName'] == mecs_flow, 'FlowAmount'].values[0]
        ghgi_energy = ghgi.loc[ghgi['FlowName'] == ghgi_flow, 'FlowAmount'].values[0]
        pct = np.minimum(mecs_energy / ghgi_energy, 1)
        pct_dict[mecs_flow] = pct

    return pct_dict


def allocate_industrial_combustion(fba, source_dict, **_):
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_df_fxn. Calculate the percentage of fuel consumption captured in
    EIA MECS relative to EPA GHGI. Create new activities to distinguish those
    which use EIA MECS as allocation source and those that use alternate source.
    """
    pct_dict = get_manufacturing_energy_ratios(source_dict.get('year'))

    # activities reflect flows in A_14 and 3_8 and 3_9
    activities_to_split = {'Industrial Other Coal Industrial': 'Coal',
                           'Natural Gas Industrial': 'Natural Gas',
                           'Coal Industrial': 'Coal',
                           'Natural gas industrial': 'Natural Gas'}

    for activity, fuel in activities_to_split.items():
        df_subset = fba.loc[fba['ActivityProducedBy'] == activity].reset_index(drop=True)
        if len(df_subset) == 0:
            continue
        df_subset['FlowAmount'] = df_subset['FlowAmount'] * pct_dict[fuel]
        df_subset['ActivityProducedBy'] = f"{activity} - Manufacturing"
        fba.loc[fba['ActivityProducedBy'] == activity,
               'FlowAmount'] = fba['FlowAmount'] * (1-pct_dict[fuel])
        fba = pd.concat([fba, df_subset], ignore_index=True)

    return fba


def split_HFCs_by_type(fba, **_):
    """Speciates HFCs and PFCs for all activities based on T_4_99.
    clean_fba_before_mapping_df_fxn"""
    splits = load_fba_w_standardized_units(datasource='EPA_GHGI_T_4_99',
                                           year=fba['Year'][0])
    splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
    splits = splits[['FlowName', 'pct']]

    speciated_df = fba.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
                            axis=1, result_type='expand')
    speciated_df.columns = splits['FlowName']
    speciated_df = pd.concat([fba, speciated_df], axis=1)
    speciated_df = speciated_df.melt(id_vars=flow_by_activity_fields.keys(),
                                     var_name='Flow')
    speciated_df['FlowName'] = speciated_df['Flow']
    speciated_df['FlowAmount'] = speciated_df['value']
    speciated_df.drop(columns=['Flow', 'value'], inplace=True)

    return speciated_df


def subtract_HFC_transport_emissions(df):
    """Remove the portion of transportation emissions which are sourced elsewhere."""
    transport_df = load_fba_w_standardized_units(datasource='EPA_GHGI_T_A_103',
                                                 year=df['Year'][0])
    activity_list = ['Mobile AC', 'Comfort Cooling for Trains and Buses',
                     'Refrigerated Transport'] # Total of all sub categories
    transport_df = transport_df[transport_df['ActivityProducedBy'].isin(activity_list)]
    df.loc[df['ActivityProducedBy'] == 'Refrigeration/Air Conditioning',
           'FlowAmount'] = df['FlowAmount'] - transport_df['FlowAmount'].sum()
    return df


def allocate_HFC_to_residential(df):
    """Split HFC emissions into two buckets to be further allocated.

    Calculate the portion of Refrigerants applied to households based on production of
    household: 335222
    industry: 333415
    """
    make_df = load_fba_w_standardized_units(datasource='BEA_Make_Detail_BeforeRedef',
                                            year=2012)
    household = make_df[(make_df['ActivityProducedBy'] == '335222') &
                        (make_df['ActivityConsumedBy'] == '335222')
                        ].reset_index()['FlowAmount'][0]
    industry = make_df[(make_df['ActivityProducedBy'] == '333415') &
                       (make_df['ActivityConsumedBy'] == '333415')
                       ].reset_index()['FlowAmount'][0]

    activity = 'Refrigeration/Air Conditioning'
    df_subset = df.loc[df['ActivityProducedBy'] == activity].reset_index(drop=True)
    df_subset['FlowAmount'] = df_subset[
        'FlowAmount'] * (household / (industry + household))
    df_subset['ActivityProducedBy'] = f"{activity} - Households"
    df.loc[df['ActivityProducedBy'] == activity,
           'FlowAmount'] = df['FlowAmount'] * (industry / (industry + household))
    df = pd.concat([df, df_subset], ignore_index=True)

    return df


def split_HFC_foams(df):
    """Split HFC emissions from foams into two buckets to be allocated separately.

    Calculate the portion for
    Polystyrene: 326140
    Urethane: 326150
    """
    make_df = load_fba_w_standardized_units(datasource='BEA_Make_Detail_BeforeRedef',
                                            year=2012)
    polystyrene = make_df[(make_df['ActivityProducedBy'] == '326140') &
                          (make_df['ActivityConsumedBy'] == '326140')
                          ].reset_index()['FlowAmount'][0]
    urethane = make_df[(make_df['ActivityProducedBy'] == '326150') &
                       (make_df['ActivityConsumedBy'] == '326150')
                       ].reset_index()['FlowAmount'][0]

    activity = 'Foams'
    df_subset = df.loc[df['ActivityProducedBy'] == activity].reset_index(drop=True)
    df_subset['FlowAmount'] = df_subset[
        'FlowAmount'] * (polystyrene / (urethane + polystyrene))
    df_subset['ActivityProducedBy'] = f"{activity} - Polystyrene"
    df.loc[df['ActivityProducedBy'] == activity, 'FlowAmount'] = df[
        'FlowAmount'] * (urethane / (urethane + polystyrene))
    df.loc[df['ActivityProducedBy'] == activity,
           'ActivityProducedBy'] = f"{activity} - Urethane"
    df = pd.concat([df, df_subset], ignore_index=True)

    return df


def clean_HFC_fba(fba, **_):
    """Adjust HFC emissions for improved parsing.
    clean_fba_before_mapping_df_fxn used in EPA_GHGI_T_4_101."""
    df = subtract_HFC_transport_emissions(fba)
    df = allocate_HFC_to_residential(df)
    df = split_HFC_foams(df)
    df = split_HFCs_by_type(df)
    return df


def remove_HFC_kt(fba, **_):
    """Remove records of emissions in kt, data are also provided in MMT CO2e.
    clean_fba_before_mapping_df_fxn used in EPA_GHGI_T_4_50."""
    return fba.loc[fba['Unit'] != 'kt']


def adjust_transport_activities(df, **_):
    """Update activity names for improved transportatin parsing.
    clean_allocation_fba used in EPA_GHGI_T_A_14"""
    activities = {'Gasoline': ['Light-Duty Trucks',
                               'Passenger Cars'],
                  'Distillate Fuel Oil (Diesel Fuel)':
                      ['Medium- and Heavy-Duty Trucks',
                       'Buses'],
                 }
    for k, v in activities.items():
        df.loc[(df['ActivityConsumedBy'].isin(v)) &
               (df['FlowName'] == k),
               'ActivityConsumedBy'] = df['ActivityConsumedBy'] + f" - {k}"
    return df


def keep_six_digit_naics(df_w_sec, **_):
    """Keep only activities at the 6-digit NAICS level
    clean_allocation_fba_w_sec used for EPA_GHGI_T_A_79"""
    df_w_sec = replace_NoneType_with_empty_cells(df_w_sec)
    df_w_sec = df_w_sec.loc[
        (df_w_sec['SectorProducedBy'].apply(lambda x: len(x) == 6)) |
        (df_w_sec['SectorConsumedBy'].apply(lambda x: len(x) == 6))]
    return df_w_sec

if __name__ == "__main__":
    import flowsa
    # fba = flowsa.getFlowByActivity('EPA_GHGI_T_4_101', 2016)
    # df = clean_HFC_fba(fba)
    fba = flowsa.flowbyactivity.main(year=2016, source='EPA_GHGI')
