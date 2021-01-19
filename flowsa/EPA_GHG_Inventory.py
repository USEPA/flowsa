# EPA_GHG_Inventory.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks-1990-2018
"""


import io
import zipfile
import numpy as np
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system

DEFAULT_YEAR = 9999

# Decided to add tables as a constant in the source code because the YML config isn't available in the ghg_call method.
# Only keeping years 2010-2018 for the following tables:
TABLES = {
    "Ch 2 - Trends": ["2-1"],
    "Ch 3 - Energy": ["3-10", "3-11", "3-14", "3-15", "3-21", "3-37", "3-38", "3-39", "3-57", "3-59", "3-22"],
    "Ch 4 - Industrial Processes": ["4-48", "4-94", "4-99", "4-101", "4-43", "4-80"],
    "Ch 5 - Agriculture": ["5-3", "5-7", "5-18", "5-19", "5-30"],
    "Executive Summary": ["ES-5"]
}
ANNEX_TABLES = {
    "Annex": ["A-17", "A-93", "A-94", "A-118"]
}
A_17_COMMON_HEADERS = ['Res.', 'Comm.', 'Ind.', 'Trans.', 'Elec.', 'Terr.', 'Total']
A_17_TBTU_HEADER = [
    'Adjusted Consumption (TBtu)a', 'Adjusted Consumption (TBtu)']
A_17_CO2_HEADER = ['Emissionsb (MMT CO2 Eq.) from Energy Use', 'Emissions (MMT CO2 Eq.) from Energy Use']

SPECIAL_FORMAT = ["3-22", "4-43", "4-80", "A-17", "A-93", "A-94", "A-118"]
SRC_NAME_SPECIAL_FORMAT = ["T_3_22", "T_4_43", "T_4_80", "T_A_17"]

DROP_COLS = ["Unnamed: 0", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998",
             "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"]

TBL_META = {
    "EPA_GHG_Inventory_T_2_1": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 2-1:  Recent Trends in U.S. Greenhouse Gas Emissions and Sinks (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_10": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-10:  CH4 Emissions from Stationary Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_11": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-11:  N2O Emissions from Stationary Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_14": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-14:  CH4 Emissions from Mobile Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_15": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-14:  CH4 Emissions from Mobile Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_21": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "TBtu",
        "desc": "Table 3-21:  Adjusted Consumption of Fossil Fuels for Non-Energy Uses (TBtu)"
    },
    "EPA_GHG_Inventory_T_3_22": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-22:  2018 Adjusted Non-Energy Use Fossil Fuel Consumption, Storage, and Emissions",
        "year": "2018"
    },
    "EPA_GHG_Inventory_T_3_37": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-37:  CH4 Emissions from Petroleum Systems (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_38": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CH4 Eq",
        "desc": "Table 3-38:  CH4 Emissions from Petroleum Systems (kt CH4)"
    },
    "EPA_GHG_Inventory_T_3_39": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-39:  CO2 Emissions from Petroleum Systems (MMT CO2)"
    },
    "EPA_GHG_Inventory_T_3_57": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-57:  CH4 Emissions from Natural Gas Systems (MMT CO2 Eq.)a"
    },
    "EPA_GHG_Inventory_T_3_59": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-59:  Non-combustion CO2 Emissions from Natural Gas Systems (MMT)"
    },
    "EPA_GHG_Inventory_T_4_43": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-43:  CO2 Emissions from Soda Ash Production"
    },
    "EPA_GHG_Inventory_T_4_80": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-80:  PFC Emissions from Aluminum Production (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_4_48": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-48:  Production of Selected Petrochemicals (kt)"
    },
    "EPA_GHG_Inventory_T_4_94": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-94:  PFC, HFC, SF6, NF3, and N2O Emissions from Electronics Manufacture [1] (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_4_99": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-99:  Emissions of HFCs and PFCs from ODS Substitutes (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_4_101": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 4-101:  Emissions of HFCs and PFCs from ODS Substitutes (MMT CO2 Eq.) by Sector"
    },
    "EPA_GHG_Inventory_T_5_3": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 5-3:  CH4 Emissions from Enteric Fermentation (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_5_7": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 5-7:  CH4 and N2O Emissions from Manure Management (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_5_18": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 5-18:  Direct N2O Emissions from Agricultural " +
                "Soils by Land Use Type and N Input Type (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_5_19": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 5-19:  Indirect N2O Emissions from Agricultural Soils (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_5_30": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 5-30:  CH4, N2O, CO, and NOx Emissions from Field Burning of Agricultural Residues (kt)"
    },
    "EPA_GHG_Inventory_T_A_17": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "2012 Energy Consumption Data and CO2 Emissions from Fossil Fuel Combustion by Fuel Type"
    },
    "EPA_GHG_Inventory_T_A_93": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "NOx Emissions from Stationary Combustion (kt)"
    },
    "EPA_GHG_Inventory_T_A_94": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "CO Emissions from Stationary Combustion (kt)"
    },
    "EPA_GHG_Inventory_T_A_118": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "NMVOCs Emissions from Mobile Combustion (kt)"
    },
    "EPA_GHG_Inventory_T_ES_5": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table ES-5:  U.S. Greenhouse Gas Emissions and Removals (Net Flux) " +
                "from Land Use, Land-Use Change, and Forestry (MMT CO2 Eq.)"
    },
}

YEARS = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"]


def ghg_url_helper(build_url, config, args):
    """Only one URL is needed to retrieve the data for all tables for all years."""
    annex_url = config['url']['annex_url']
    return [build_url, annex_url]


def ghg_call(url, response, args):
    """
    Callback function for the US GHG Emissions download. Open the downloaded zip file and
    read the contained CSV(s) into pandas dataframe(s).
    """
    df = None
    year = args['year']
    with zipfile.ZipFile(io.BytesIO(response.content), "r") as f:
        frames = []
        # TODO: replace this TABLES constant with kwarg['tables']
        if 'annex' in url:
            is_annex = True
            t_tables = ANNEX_TABLES
        else:
            is_annex = False
            t_tables = TABLES
        for chapter, tables in t_tables.items():
            for table in tables:
                # path = os.path.join("Chapter Text", chapter, f"Table {table}.csv")
                if is_annex:
                    path = f"Annex/Table {table}.csv"
                else:
                    path = f"Chapter Text/{chapter}/Table {table}.csv"
                data = f.open(path)
                if table not in SPECIAL_FORMAT:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", thousands=",")
                elif '3-' in table:
                    # Skip first two rows, as usual, but make headers the next 3 rows:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", header=[0, 1, 2], thousands=",")
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
                    print('break')
                elif '4-' in table:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", thousands=",", decimal=".")
                elif 'A-' in table:
                    if table == 'A-17':
                        # A-17  is similar to T 3-23, the entire table is 2012 and headings are completely different.
                        if str(year) == '2012':
                            df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", header=[0, 1], thousands=",")
                            new_headers = []
                            header_grouping = ''
                            for col in df.columns:
                                if 'Unnamed' in col[0]:
                                    # new_headers.append(f'{header_grouping}{col[1]}')
                                    new_headers.append(f'{col[1].strip()}{header_grouping}')
                                else:
                                    if len(col) == 2:
                                        # header_grouping = f'{col[0]}__'
                                        if col[0] == A_17_TBTU_HEADER[0]:
                                            header_grouping = f' {A_17_TBTU_HEADER[1].strip()}'
                                        else:
                                            header_grouping = f' {A_17_CO2_HEADER[0].strip()}'
                                    # new_headers.append(f'{header_grouping}{col[1]}')
                                    new_headers.append(f'{col[1].strip()}{header_grouping}')
                            df.columns = new_headers
                            df['Year'] = year
                    else:
                        df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1", thousands=",", decimal=".")

                if df is not None and len(df.columns) > 1:
                    years = YEARS.copy()
                    years.remove(str(year))
                    df = df.drop(columns=(DROP_COLS + years), errors='ignore')
                    # Assign SourceName now while we still have access to the table name:
                    source_name = f"EPA_GHG_Inventory_T_{table.replace('-', '_')}"
                    df["SourceName"] = source_name
                    frames.append(df)

        # return pd.concat(frames)
        return frames


def get_unnamed_cols(df):
    """Get a list of all unnamed columns, used to drop them."""
    return [col for col in df.columns if "Unnamed" in col]


def ghg_parse(dataframe_list, args):
    """ TODO. """
    cleaned_list = []
    for df in dataframe_list:
        special_format = False
        source_name = df["SourceName"][0]
        print(f'Processing Source Name {source_name}')
        for src in SRC_NAME_SPECIAL_FORMAT:
            if src in source_name:
                special_format = True

        # Specify to ignore errors in case one of the drop_cols is missing.
        drop_cols = get_unnamed_cols(df)
        df = df.drop(columns=drop_cols, errors='ignore')

        if not special_format or "T_4_" not in source_name:
            # Rename the PK column from data_type to "ActivityProducedBy":
            df = df.rename(columns={df.columns[0]: "ActivityProducedBy"})
        else:
            df["ActivityProducedBy"] = 'None'

        df["ActivityConsumedBy"] = 'None'
        df["FlowType"] = "ELEMENTARY_FLOW"
        df["Location"] = "00000"

        id_vars = ["SourceName", "ActivityConsumedBy", "ActivityProducedBy", "FlowType", "Location"]
        if special_format and "Year" in df.columns:
            id_vars.append("Year")
            # Cast Year column to numeric and delete any years <= 2009:
            # df = df[pd.to_numeric(df["Year"], errors="coerce") > 2009]
            # Cast Year column to numeric and delete any years != year
            df = df[pd.to_numeric(df["Year"], errors="coerce") == int(args['year'])]

        # Set index on the df:
        df.set_index(id_vars)

        if special_format:
            if "T_4_" not in source_name:
                df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
            else:
                df.drop(columns=["ActivityProducedBy"], errors="ignore")
                df = df.melt(id_vars=id_vars, var_name="ActivityProducedBy", value_name="FlowAmount")
        else:
            df = df.melt(id_vars=id_vars, var_name="Year", value_name="FlowAmount")

        # Dropping all rows with value "+"
        try:
            df = df[~df["FlowAmount"].str.contains("\\+", na=False)]
        except AttributeError as ex:
            print(ex)
        # Dropping all rows with value "NE"
        try:
            df = df[~df["FlowAmount"].str.contains("NE", na=False)]
        except AttributeError as ex:
            print(ex)

        # Convert all empty cells to nan cells
        df["FlowAmount"].replace("", np.nan, inplace=True)
        # Table 3-10 has some NO values, dropping these.
        df["FlowAmount"].replace("NO", np.nan, inplace=True)
        # Table A-118 has some IE values, dropping these.
        df["FlowAmount"].replace("IE", np.nan, inplace=True)

        # Drop any nan rows
        df.dropna(subset=['FlowAmount'], inplace=True)

        df["Description"] = 'None'
        df["Unit"] = "Other"

        # Special case for T_A_17: ActivityConsumedBy and FlowName based on that value.
        if "T_A_17" in source_name:
            df['ActivityConsumedBy'] = df['FlowName']

        # Update classes:
        # TODO: replace this TBL_META constant with kwargs['tbl_meta']
        meta = TBL_META[source_name]
        df.loc[df["SourceName"] == source_name, "Class"] = meta["class"]
        df.loc[df["SourceName"] == source_name, "Unit"] = meta["unit"]
        df.loc[df["SourceName"] == source_name, "Description"] = meta["desc"]
        df.loc[df["SourceName"] == source_name, "Compartment"] = meta["compartment"]
        # if not special_format or "T_4_" not in source_name or "T_4_80" in source_name:
        df.loc[df["SourceName"] == source_name, "FlowName"] = meta["flow_name"]
        if 'Year' not in df.columns:
            df['Year'] = meta.get("year", DEFAULT_YEAR)

        # Special case for T_A_17: ActivityConsumedBy and FlowName based on that value.
        if "T_A_17" in source_name:
            # Update FlowName based on the (x) contents.
            for header in A_17_COMMON_HEADERS:
                df.loc[df["ActivityConsumedBy"] == f'{header} {A_17_TBTU_HEADER[1]}', "FlowName"] = 'TBtu'
            # df.loc['' in df["ActivityConsumedBy"], "FlowName"] = 'CO2 Eq'

        # Some of the datasets, 4-43 and 4-80, still have years we don't want at this point.
        # Remove rows matching the years we don't want:
        try:
            df = df[df['Year'].isin([args['year']])]
        except AttributeError as ex:
            print(ex)

        # Add tmp DQ scores
        df["DataReliability"] = 5
        df["DataCollection"] = 5
        # Fill in the rest of the Flow by fields so they show "None" instead of nan.76i
        df["MeasureofSpread"] = 'None'
        df["DistributionType"] = 'None'
        df["LocationSystem"] = 'None'

        df = assign_fips_location_system(df, str(args['year']))

        df = df.loc[:, ~df.columns.duplicated()]
        cleaned_list.append(df)

    if cleaned_list:
        df = pd.concat(cleaned_list)
        # Remove commas from numbers again in case any were missed:
        df["FlowAmount"].replace(',', '', regex=True, inplace=True)
    else:
        df = pd.DataFrame()

    return df

