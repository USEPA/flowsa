# EPA_GHGI.py (flowsa)
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
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units, dynamically_import_fxn
from flowsa.dataclean import replace_NoneType_with_empty_cells
from flowsa.settings import log, externaldatapath
from flowsa.schema import flow_by_activity_fields

A_17_COMMON_HEADERS = ['Res.', 'Comm.', 'Ind.', 'Trans.', 'Elec.', 'Terr.', 'Total']
A_17_TBTU_HEADER = ['Adjusted Consumption (TBtu)a', 'Adjusted Consumption (TBtu)']
A_17_CO2_HEADER = ['Emissionsb (MMT CO2 Eq.) from Energy Use',
                   'Emissions (MMT CO2 Eq.) from Energy Use']

A_10_TBTU_1_HEADER = ["Total Consumption (TBtu) a", "Total Consumption (TBtu)"]
A_10_TBTU_2_HEADER = ["Adjustments (TBtu) b", "Adjustments (TBtu)"]
A_10_TBTU_3_HEADER = ["Total Adjusted Consumption (TBtu)"]

A_TBTU_HEADER = ["Adjusted Consumption (TBtu) a", "Adjusted Consumption (TBtu)"]
A_CO2_HEADER = ["Emissions b (MMT CO2 Eq.) from Energy Use", "Emissions (MMT CO2 Eq.) from Energy Use"]

SPECIAL_FORMAT = ["3-10", "3-22", "4-46", "4-50", "4-80", "A-10", "A-11", "A-12", "A-13", "A-14", "A-15", "A-16",
                  "A-17", "A-18", "A-19", "A-20", "A-93", "A-94", "A-118", "5-29"]
SRC_NAME_SPECIAL_FORMAT = ["T_3_22", "T_4_43", "T_4_80", "T_A_17"]
Activity_Format_A = ["T_5_30", "T_A_17", "T_ES_5"]
Activity_Format_B = ["T_2_1", "T_3_21", "T_3_22", "T_4_48", "T_5_18"]
A_Table_List = ["A-11", "A-12", "A-13", "A-14", "A-15", "A-16", "A-17", "A-18", "A-19", "A-20"]

DROP_COLS = ["Unnamed: 0", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998",
             "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"]

YEARS = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"]


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


def fix_a17_headers(header):
    """
    Fix A-17 headers, trim white spaces, convert shortened words such as Elec., Res., etc.
    :param header: str, column header
    :return: str, modified column header
    """
    if header == A_17_TBTU_HEADER[0]:
        header = f' {A_17_TBTU_HEADER[1].strip()}'.replace('')
    elif header == A_17_CO2_HEADER[0]:
        header = f' {A_17_CO2_HEADER[1].strip()}'
    else:
        header = header.strip()
        header = header.replace('Res.', 'Residential')
        header = header.replace('Comm.', 'Commercial')
        header = header.replace('Ind.', 'Industrial')
        header = header.replace('Trans.', 'Transportation')
        header = header.replace('Elec.', 'Electricity Power')
        header = header.replace('Terr.', 'U.S. Territory')
    return header


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
    return default_flow_name.replace('__type__', name.strip())


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

def annex_yearly_tables(data):
    df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                     header=[0, 1], thousands=",")
    header_name = ""
    newcols = []  # empty list to have new column names
    for i in range(len(df.columns)):
        fuel_type = str(df.iloc[0, i])
        fuel_type = fuel_type.replace('Res.', 'Residential')
        fuel_type = fuel_type.replace('Comm.', 'Commercial')
        fuel_type = fuel_type.replace('Ind.', 'Industrial')
        fuel_type = fuel_type.replace('Trans.', 'Transportation')
        fuel_type = fuel_type.replace('Elec.', 'Electricity Power')
        fuel_type = fuel_type.replace('Terr.', 'U.S. Territory')
        fuel_type = fuel_type.strip()

        col_name = df.columns[i][1]
        if "Unnamed" in col_name:
            column_name = header_name
        elif col_name == A_TBTU_HEADER[0]:
            column_name = A_TBTU_HEADER[1]
            header_name = A_TBTU_HEADER[1]
        elif col_name == A_CO2_HEADER[0]:
            column_name = A_CO2_HEADER[1]
            header_name = A_CO2_HEADER[1]

        newcols.append(column_name + ' - ' + fuel_type)  # make and add new name to list
    df.columns = newcols  # assign column names
    df = df.iloc[1:, :]  # exclude first row
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
                # path = os.path.join("Chapter Text", chapter, f"Table {table}.csv")
                if is_annex:
                    path = f"Annex/Table {table}.csv"
                else:
                    path = f"Chapter Text/{chapter}/Table {table}.csv"
                if table != "3-22b":
                    data = f.open(path)
                if table not in SPECIAL_FORMAT and table != "3-22b":
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", thousands=",")
                elif '3-' in table:
                    if table == '3-10':
                        df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                         thousands=",", decimal=".")
                    elif table != "3-22b":
                        if table == '3-22' and str(year) != '2019':
                            df = None
                            continue
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
                    else: # table == "3-22b"
                        if str(year) != '2019':
                            df = pd.read_csv(f"{externaldatapath}/GHGI_Table_{table}.csv",
                                             skiprows=2, encoding="ISO-8859-1", thousands=",")
                        else:
                            df = None
                elif '4-' in table:
                    if table == '4-46':
                        df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                         thousands=",", decimal=".")
                    else:
                        df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                         thousands=",", decimal=".")
                elif 'A-' in table:
                    if table == 'A-17':
                        # A-17  is similar to T 3-23, the entire table is 2012 and
                        # headings are completely different.
                        if str(year) == '2013':
                            df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                             header=[0, 1], thousands=",")
                            new_headers = []
                            header_grouping = ''
                            for col in df.columns:
                                if 'Unnamed' in col[0]:
                                    # new_headers.append(f'{header_grouping}{col[1]}')
                                    new_headers.append(f'{fix_a17_headers(col[1])}'
                                                       f'{header_grouping}')
                                else:
                                    if len(col) == 2:
                                        # header_grouping = f'{col[0]}__'
                                        if col[0] == A_17_TBTU_HEADER[0]:
                                            header_grouping = f' {A_17_TBTU_HEADER[1].strip()}'
                                        else:
                                            header_grouping = f' {A_17_CO2_HEADER[1].strip()}'
                                    # new_headers.append(f'{header_grouping}{col[1]}')
                                    new_headers.append(f'{fix_a17_headers(col[1])}'
                                                       f'{header_grouping}')
                            df.columns = new_headers
                            nan_col = 'Electricity Power Emissions (MMT CO2 Eq.) from Energy Use'
                            fill_col = 'Unnamed: 12_level_1 Emissions (MMT CO2 Eq.) from Energy Use'
                            df = df.drop(nan_col, 1)
                            df.columns = [nan_col if x == fill_col else x for x in df.columns]
                            df['Year'] = year
                        else:
                            df = None
                    elif table == 'A-10':
                        # A-17  is similar to T 3-23, the entire table is 2012 and
                        # headings are completely different.
                        if str(year) == '2019':
                            df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                             header=[0, 1], thousands=",")
                            df = df.drop([0])
                            new_headers = []
                            header_grouping = ''
                            header_name = ""
                            newcols = []  # empty list to have new column names
                            for i in range(len(df.columns)):
                                fuel_type = str(df.iloc[0, i])
                                fuel_type = fuel_type.replace('Res.', 'Residential')
                                fuel_type = fuel_type.replace('Comm.', 'Commercial')
                                fuel_type = fuel_type.replace('Ind.', 'Industrial Other')
                                fuel_type = fuel_type.replace('Trans.', 'Transportation')
                                fuel_type = fuel_type.replace('Elec.', 'Electricity Power')
                                fuel_type = fuel_type.replace('Terr.', 'U.S. Territory')
                                fuel_type = fuel_type.strip()

                                col_name = df.columns[i][1]
                                if "Unnamed" in col_name:
                                    column_name = header_name
                                elif col_name == A_10_TBTU_1_HEADER[0]:
                                    column_name = A_10_TBTU_1_HEADER[1]
                                    header_name = A_10_TBTU_1_HEADER[1]
                                elif col_name == A_10_TBTU_2_HEADER[0]:
                                    column_name = A_10_TBTU_2_HEADER[1]
                                    header_name = A_10_TBTU_2_HEADER[1]
                                elif col_name == A_10_TBTU_3_HEADER[0]:
                                    header_name = A_10_TBTU_3_HEADER[0]

                                newcols.append(column_name + ' - ' + fuel_type)  # make and add new name to list
                            df.columns = newcols  # assign column names
                            df = df.iloc[1:, :]  # exclude first row
                            df['Year'] = year
                            df = df.reset_index(drop=True)
                    elif table == 'A-11':
                        if str(year) == '2019':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-12':
                        if str(year) == '2018':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-13':
                        if str(year) == '2017':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-14':
                        if str(year) == '2016':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-15':
                        if str(year) == '2015':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-16':
                        if str(year) == '2014':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-18':
                        if str(year) == '2012':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-19':
                        if str(year) == '2011':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    elif table == 'A-20':
                        if str(year) == '2010':
                            df = annex_yearly_tables(data)
                        else:
                            df = None
                    else:
                        df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                         thousands=",", decimal=".")
                elif '5-' in table:
                    df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                     thousands=",", decimal=".")

                if df is not None and len(df.columns) > 1:
                    years = YEARS.copy()
                    years.remove(str(year))
                    df = df.drop(columns=(DROP_COLS + years), errors='ignore')
                    # Assign SourceName now while we still have access to the table name:
                    df["SourceName"] = f"EPA_GHGI_T_{table.replace('-', '_')}"
                    frames.append(df)

        # return pd.concat(frames)
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
    notes = [" a ", " b ", " c ", " d ", " e ", " f ", " g ", " h ", " i ", " j ", " k ", " b,c ", " h,i ", " f,g "]
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
        special_format = False
        source_name = df["SourceName"][0]
        log.info('Processing Source Name %s', source_name)
        for src in SRC_NAME_SPECIAL_FORMAT:
            if src in source_name:
                special_format = True

        # Specify to ignore errors in case one of the drop_cols is missing.
        drop_cols = get_unnamed_cols(df)
        df = df.drop(columns=drop_cols, errors='ignore')
        is_cons = is_consumption(source_name, config)
        if not special_format or "T_4_" not in source_name:
            # Rename the PK column from data_type to "ActivityProducedBy" or "ActivityConsumedBy":
            if is_cons:
                df = df.rename(columns={df.columns[0]: "ActivityConsumedBy"})
                df["ActivityProducedBy"] = 'None'
            else:
                df = df.rename(columns={df.columns[0]: "ActivityProducedBy"})
                df["ActivityConsumedBy"] = 'None'
        else:
            df["ActivityConsumedBy"] = 'None'
            df["ActivityProducedBy"] = 'None'


        df["FlowType"] = "ELEMENTARY_FLOW"
        df["Location"] = "00000"
        annex_tables = ["EPA_GHGI_T_A_10", "EPA_GHGI_T_A_11", "EPA_GHGI_T_A_12", "EPA_GHGI_T_A_13", "EPA_GHGI_T_A_14",
                        "EPA_GHGI_T_A_15", "EPA_GHGI_T_A_16", "EPA_GHGI_T_A_18", "EPA_GHGI_T_A_19", "EPA_GHGI_T_A_20"]

        id_vars = ["SourceName", "ActivityConsumedBy", "ActivityProducedBy", "FlowType", "Location"]
        if special_format and "Year" in df.columns:
            id_vars.append("Year")
            # Cast Year column to numeric and delete any years != year
            df = df[pd.to_numeric(df["Year"], errors="coerce") == int(year)]

        # Set index on the df:
        df.set_index(id_vars)
        switch_year_apb = ["EPA_GHGI_T_4_14", "EPA_GHGI_T_4_50"]
        if special_format:
            if "T_4_" not in source_name:
                df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
            else:
                df = df.melt(id_vars=id_vars, var_name="Units", value_name="FlowAmount")
        elif source_name in annex_tables:
            if source_name == "EPA_GHGI_T_A_10":
                df = df.drop(columns=['Year'])
            df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
            df["Year"] = year
        else:
            df = df.melt(id_vars=id_vars, var_name="Year", value_name="FlowAmount")
            if source_name in switch_year_apb:
                df = df.rename(columns={'ActivityProducedBy': 'Year', 'Year': 'ActivityProducedBy'})

        if source_name in annex_tables:
            for index, row in df.iterrows():
                name = df.loc[index, 'FlowName']
                if source_name == "EPA_GHGI_T_A_17":
                    if "Other" in name:
                        name_split = name.split(" Other")
                        df.loc[index, 'ActivityConsumedBy'] = df.loc[index, 'ActivityConsumedBy'] + " - " + name_split[
                            0]
                    elif "Emissions" in name:
                        name_split = name.split(" Emissions")
                        df.loc[index, 'ActivityConsumedBy'] = df.loc[index, 'ActivityConsumedBy'] + " - " + name_split[
                            0]
                    elif "Adjusted" in name:
                        name_split = name.split(" Adjusted")
                        df.loc[index, 'ActivityConsumedBy'] = df.loc[index, 'ActivityConsumedBy'] + " - " + name_split[
                            0]
                elif source_name in annex_tables:
                    name_split = name.split(" (")

                    df.loc[index, 'ActivityConsumedBy'] = (
                        f"{str(df.loc[index, 'ActivityConsumedBy'])}"
                        f" {name_split[1].split('- ')[1]}"
                    )
                    if name_split[0] == "Emissions":
                        df.loc[index, 'FlowName'] = "CO2"
                        df.loc[index, 'Unit'] = "MMT CO2e"
                        df.loc[index, 'Class'] = "Chemicals"
                    else:
                        df.loc[index, 'FlowName'] = "Energy Consumption"
                        df.loc[index, 'Unit'] = "TBtu"
                        df.loc[index, 'Class'] = "Energy"

        # Dropping all rows with value "+"
        try:
            df = df[~df["FlowAmount"].str.contains("\\+", na=False)]
        except AttributeError as ex:
            log.info(ex)
        # Dropping all rows with value "NE"
        try:
            df = df[~df["FlowAmount"].str.contains("NE", na=False)]
        except AttributeError as ex:
            log.info(ex)

        # Convert all empty cells to nan cells
        df["FlowAmount"].replace("", np.nan, inplace=True)
        # Table 3-10 has some NO values, dropping these.
        df["FlowAmount"].replace("NO", np.nan, inplace=True)
        # Table A-118 has some IE values, dropping these.
        df["FlowAmount"].replace("IE", np.nan, inplace=True)
        df["FlowAmount"].replace(r'NO ', np.nan, inplace=True)

        # Drop any nan rows
        df.dropna(subset=['FlowAmount'], inplace=True)

        df["Description"] = 'None'
        if source_name not in annex_tables:
            df["Unit"] = "Other"

        # Update classes:
        meta = get_table_meta(source_name, config)
        if source_name == "EPA_GHGI_T_3_21" and int(year) < 2015:
            # skip don't do anything: The lines are blank
            print("There is no data for this year and source")
        elif source_name not in annex_tables:
            df.loc[df["SourceName"] == source_name, "Class"] = meta["class"]
            df.loc[df["SourceName"] == source_name, "Unit"] = meta["unit"]
            df.loc[df["SourceName"] == source_name, "Description"] = meta["desc"]
            df.loc[df["SourceName"] == source_name, "Compartment"] = meta["compartment"]
            if not special_format or "T_4_" in source_name:
                df.loc[df["SourceName"] == source_name, "FlowName"] = meta["activity"]
            else:
                if "T_4_" not in source_name:
                    flow_name_units = series_separate_name_and_units(df["FlowName"],
                                                                     meta["activity"],
                                                                     meta["unit"])
                    df['Unit'] = flow_name_units['units']
                    df.loc[df["SourceName"] == source_name, "FlowName"] = flow_name_units['names']

        # We also need to fix the Activity PRODUCED or CONSUMED, now that we know units.
        # Any units TBtu will be CONSUMED, all other units will be PRODUCED.
        if is_cons:
            df['ActivityProducedBy'] = df['ActivityConsumedBy']
            df.loc[df["Unit"] == 'TBtu', 'ActivityProducedBy'] = 'None'
            df.loc[df["Unit"] != 'TBtu', 'ActivityConsumedBy'] = 'None'
        else:
            df['ActivityConsumedBy'] = df['ActivityProducedBy']
            df.loc[df["Unit"] == 'TBtu', 'ActivityProducedBy'] = 'None'
            df.loc[df["Unit"] != 'TBtu', 'ActivityConsumedBy'] = 'None'

        if 'Year' not in df.columns:
            df['Year'] = year

        if source_name == "EPA_GHGI_T_4_33":
            df = df.rename(columns={'Year': 'ActivityProducedBy', 'ActivityProducedBy': 'Year'})
        year_int = ["EPA_GHGI_T_4_33", "EPA_GHGI_T_4_50"]
        # Some of the datasets, 4-43 and 4-80, still have years we don't want at this point.
        # Remove rows matching the years we don't want:
        try:
            if source_name in year_int:
                df = df[df['Year'].isin([int(year)])]
            else:
                df = df[df['Year'].isin([year])]

        except AttributeError as ex:
            log.info(ex)

        # Add DQ scores
        df["DataReliability"] = 5  # tmp
        df["DataCollection"] = 5  # tmp
        # Fill in the rest of the Flow by fields so they show "None" instead of nan.76i
        df["MeasureofSpread"] = 'None'
        df["DistributionType"] = 'None'
        df["LocationSystem"] = 'None'

        df = assign_fips_location_system(df, str(year))
        modified_activity_list = ["EPA_GHGI_T_ES_5"]
        multi_chem_names = ["EPA_GHGI_T_2_1", "EPA_GHGI_T_4_46", "EPA_GHGI_T_5_7",
                            "EPA_GHGI_T_5_29", "EPA_GHGI_T_ES_5"]
        source_No_activity = ["EPA_GHGI_T_3_22", "EPA_GHGI_T_3_22b"]
        source_activity_1 = ["EPA_GHGI_T_3_8", "EPA_GHGI_T_3_9", "EPA_GHGI_T_3_14", "EPA_GHGI_T_3_15",
                             "EPA_GHGI_T_5_3", "EPA_GHGI_T_5_18", "EPA_GHGI_T_5_19", "EPA_GHGI_T_A_76",
                             "EPA_GHGI_T_A_77", "EPA_GHGI_T_3_10", "EPA_GHGI_T_A_103"]
        source_activity_2 =  ["EPA_GHGI_T_3_38", "EPA_GHGI_T_3_63"]
        double_activity = ["EPA_GHGI_T_4_48"]
        note_par = ["EPA_GHGI_T_4_14", "EPA_GHGI_T_4_99"]
        if source_name in multi_chem_names:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["CO2", "CH4", "N2O", "NF3", "HFCs", "PFCs", "SF6", "NF3", "CH4 a", "N2O b", "CO", "NOx"]
            for index, row in df.iterrows():
                apb_value = row["ActivityProducedBy"]
                if "CH4" in apb_value and "LULUCF" not in apb_value:
                    apb_value = "CH4"
                elif "N2O" in apb_value and apb_value != "N2O from Product Uses"\
                        and "LULUCF" not in apb_value:
                    apb_value = "N2O"
                elif "CO2" in apb_value:
                    apb_value = "CO2"

                if apb_value in flow_name_list and apb_value != "N2O from Product Uses"\
                        and "LULUCF" not in apb_value:
                    apbe_value = apb_value
                    df.loc[index, 'FlowName'] = apbe_value
                    df.loc[index, 'ActivityProducedBy'] = "All activities"
                    bool_apb = True
                else:
                    if bool_apb == True:
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'] = apb_txt
                        df.loc[index, 'FlowName'] = apbe_value
                    else:
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'] = apb_txt

                if "Total" == apb_value or "Total " == apb_value:
                  df = df.drop(index)
            if source_name == "EPA_GHGI_T_ES_5":
                df = df.rename(columns={'FlowName': 'ActivityProducedBy', 'ActivityProducedBy': 'FlowName'})
        elif source_name in source_No_activity:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["Industry", "Transportation", "U.S. Territories"]
            for index, row in df.iterrows():
                unit = row["Unit"]
                if unit.strip() == "MMT  CO2":
                        df.loc[index, 'Unit'] = "MMT CO2e"
                if df.loc[index, 'Unit'] != "MMT CO2e":
                    df = df.drop(index)
                else:
                    apb_value = row["ActivityProducedBy"]
                    if apb_value in flow_name_list:
                        apbe_value = apb_value
                        if apb_value == "U.S. Territories":
                            df.loc[index, 'Location'] = "99000"
                        df.loc[index, 'FlowName'] = "CO2"
                        df.loc[index, 'ActivityProducedBy'] = apbe_value + " " + "All activities"
                        bool_apb = True
                    else:
                        if bool_apb == True:
                            df.loc[index, 'FlowName'] = "CO2"
                            apb_txt = df.loc[index, 'ActivityProducedBy']
                            apb_txt = strip_char(apb_txt)
                            if apbe_value == "U.S. Territories":
                                df.loc[index, 'Location'] = "99000"
                            df.loc[index, 'ActivityProducedBy'] = apbe_value + " " + apb_txt
                        else:
                            apb_txt = df.loc[index, 'ActivityProducedBy']
                            apb_txt = strip_char(apb_txt)
                            df.loc[index, 'ActivityProducedBy'] = apbe_value + " " + apb_txt
                        if "Total" == apb_value or "Total " == apb_value:
                            df = df.drop(index)
        elif source_name in source_activity_1:
            bool_apb = False
            apbe_value = ""
            activity_subtotal = ["Electric Power", "Industrial", "Commercial", "Residential", "U.S. Territories",
                                 "U.S. Territories a", "Transportation",
                                 "Fuel Type/Vehicle Type a", "Diesel On-Road b",
                                 "Alternative Fuel On-Road", "Non-Road c",
                                 "Gasoline On-Road b", "Non-Road", "Exploration a",
                                 "Production (Total)", "Crude Oil Transportation", "Refining",
                                 "Exploration b", "Cropland", "Grassland"]
            for index, row in df.iterrows():
                apb_value = row["ActivityProducedBy"]
                start_activity = row["FlowName"]
                if apb_value in activity_subtotal:
                    if "U.S. Territories" in apb_value:
                        df.loc[index, 'Location'] = "99000"
                    elif "U.S. Territories" in apbe_value:
                        df.loc[index, 'Location'] = "99000"
                    apbe_value = apb_value
                    apbe_value = strip_char(apbe_value)
                    df.loc[index, 'FlowName'] = start_activity
                    df.loc[index, 'ActivityProducedBy'] = "All activities" + " " + apbe_value
                    bool_apb = True
                else:
                    if bool_apb == True:
                        if "U.S. Territories" in apb_value:
                            df.loc[index, 'Location'] = "99000"
                        elif "U.S. Territories" in apbe_value:
                            df.loc[index, 'Location'] = "99000"
                        df.loc[index, 'FlowName'] = start_activity
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'] = apb_txt + " " + apbe_value
                        if source_name == "EPA_GHGI_T_3_10":
                            df.loc[index, 'FlowName'] = apb_txt
                    else:
                        if "U.S. Territories" in apb_value:
                            df.loc[index, 'Location'] = "99000"
                        elif "U.S. Territories" in apbe_value:
                            df.loc[index, 'Location'] = "99000"
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        apb_final = apb_txt + " " + apbe_value
                        df.loc[index, 'ActivityProducedBy'] = apb_final.strip()
                if "Total" == apb_value or "Total " == apb_value:
                  df = df.drop(index)
        elif source_name in source_activity_2:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["Explorationb", "Production", "Processing", "Transmission and Storage", "Distribution",
                              "Crude Oil Transportation", "Refining", "Exploration" ]
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
                        df.loc[index, 'ActivityProducedBy'] = apbe_value + " - " + apb_txt
                    else:
                        apb_txt = df.loc[index, 'ActivityProducedBy']
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'] = apb_txt + " " + apbe_value
                if "Total" == apb_value or "Total " == apb_value:
                  df = df.drop(index)
        elif source_name in double_activity:
            for index, row in df.iterrows():
                df.loc[index, 'FlowName'] = df.loc[index, 'ActivityProducedBy']
        elif source_name in annex_tables:
            for index, row in df.iterrows():
                if df.loc[index, 'ActivityProducedBy'] == "None":
                    df.loc[index, 'Unit'] = "TBtu"
                    df.loc[index, 'FlowName'] = "Energy Consumption"
                else:
                    df.loc[index, 'Unit'] = "MMT CO2e"
                    df.loc[index, 'FlowName'] = "CO2"
                    df.loc[index, 'Compartment'] = 'air'
        elif source_name == "EPA_GHGI_T_A_79":
            df = df.rename(
                columns={'ActivityProducedBy': 'ActivityConsumedBy', 'ActivityConsumedBy': 'ActivityProducedBy'})
            fuel_name = ""
            A_79_unit_dict = {'Natural Gas': 'trillion cubic feet',
                              'Electricity': 'million kilowatt-hours'}
            for index, row in df.iterrows():
                if row["ActivityConsumedBy"].startswith(' '): # indicates subcategory
                    df.loc[index, 'ActivityConsumedBy'] = strip_char(df.loc[index, 'ActivityConsumedBy'])
                    df.loc[index, 'FlowName'] = fuel_name
                else: # fuel header
                    fuel_name = df.loc[index, 'ActivityConsumedBy']
                    fuel_name = strip_char(fuel_name)
                    df.loc[index, 'ActivityConsumedBy'] = "All activities"
                    df.loc[index, 'FlowName'] = fuel_name
                if fuel_name in A_79_unit_dict.keys():
                    df.loc[index, 'Unit'] = A_79_unit_dict[fuel_name]
        else:
            if source_name in "EPA_GHGI_T_4_80":
                for index, row in df.iterrows():
                    df.loc[index, 'FlowName'] = df.loc[index, 'Units']
                    df.loc[index, 'ActivityProducedBy'] = "Aluminum Production"
            elif source_name in "EPA_GHGI_T_4_84":
                for index, row in df.iterrows():
                    df.loc[index, 'FlowName'] = df.loc[index, 'ActivityProducedBy']
                    df.loc[index, 'ActivityProducedBy'] = "Magnesium Production and Processing"
            elif source_name in "EPA_GHGI_T_4_94":
                for index, row in df.iterrows():
                    df.loc[index, 'FlowName'] = df.loc[index, 'ActivityProducedBy']
                    df.loc[index, 'ActivityProducedBy'] = "Electronics Production"
            elif source_name in "EPA_GHGI_T_4_99":
                for index, row in df.iterrows():
                    df.loc[index, 'FlowName'] = df.loc[index, 'ActivityProducedBy']
                    df.loc[index, 'ActivityProducedBy'] = "ODS Substitute"
            elif source_name in "EPA_GHGI_T_4_33":
                for index, row in df.iterrows():
                    df.loc[index, 'Unit'] = df.loc[index, 'ActivityProducedBy']
                    df.loc[index, 'ActivityProducedBy'] = "Caprolactam Production"
            elif source_name in "EPA_GHGI_T_A_101":
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    df.loc[index, 'ActivityProducedBy'] = apb_value
            elif source_name == "EPA_GHGI_T_4_50":
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    df.loc[index, 'ActivityProducedBy'] = "HCFC-22 Production"
                    if "kt" in apb_value:
                        df.loc[index, 'Unit'] = "kt"
                    else:
                        df.loc[index, 'Unit'] = "MMT CO2e"
            elif source_name in note_par:
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    if "(" in apb_value:
                        text_split = apb_value.split("(")
                        df.loc[index, 'ActivityProducedBy'] = text_split[0]
            else:
                for index, row in df.iterrows():
                    if "CO2" in df.loc[index, 'Unit']:
                        if source_name == "EPA_GHGI_T_3_42" or source_name == "EPA_GHGI_T_3_67":
                            df.loc[index, 'Unit'] = df.loc[index, 'Unit']
                        else:
                            df.loc[index, 'Unit'] = "MMT CO2e"
                    if "U.S. Territory" in df.loc[index, 'ActivityProducedBy']:
                        df.loc[index, 'Location'] = "99000"

            df.drop(df.loc[df['ActivityProducedBy'] == "Total"].index, inplace=True)
            df.drop(df.loc[df['ActivityProducedBy'] == "Total "].index, inplace=True)
            df.drop(df.loc[df['FlowName'] == "Total"].index, inplace=True)
            df.drop(df.loc[df['FlowName'] == "Total "].index, inplace=True)


        if source_name in modified_activity_list:

            if is_cons:
                df = df.rename(columns={'FlowName': 'ActivityConsumedBy', 'ActivityConsumedBy': 'FlowName'})
            else:
                df = df.rename(columns={'FlowName': 'ActivityProducedBy', 'ActivityProducedBy': 'FlowName'})

        df = df.loc[:, ~df.columns.duplicated()]
        cleaned_list.append(df)

    if cleaned_list:
        for df in cleaned_list:
            # Remove commas from numbers again in case any were missed:
            df["FlowAmount"].replace(',', '', regex=True, inplace=True)
        return cleaned_list
        # df = pd.concat(cleaned_list)
    else:
        df = pd.DataFrame()
        return df


def get_manufacturing_energy_ratios(year):
    """Calculate energy ratio by fuel between GHGI and EIA MECS."""
    # activity correspondence between GHGI and MECS
    activities_corr = {'Industrial Other Coal Industrial': 'Coal',
                       'Natural Gas  Industrial': 'Natural Gas',  # note extra space
                       }

    # TODO make this year dynamic
    # Filter MECS for total national energy consumption for manufacturing sectors
    mecs = load_fba_w_standardized_units(datasource='EIA_MECS_Energy',
                                         year=year,
                                         flowclass='Energy')
    mecs = mecs.loc[(mecs['ActivityConsumedBy'] == '31-33') &
                    (mecs['Location'] == '00000')].reset_index(drop=True)
    mecs = dynamically_import_fxn('EIA_MECS_Energy',
                                  'mecs_energy_fba_cleanup')(mecs, None)

    # TODO dynamically change the table imported here based on year
    ghgi = load_fba_w_standardized_units(datasource='EPA_GHGI_T_A_14',
                                         year=2016,
                                         flowclass='Energy')

    pct_dict = {}
    for sector, fuel in activities_corr.items():
        # Calculate percent energy contribution from MECS based on v
        mecs_energy = mecs.loc[mecs['FlowName'] == fuel, 'FlowAmount'].values[0]
        ghgi_energy = ghgi.loc[ghgi['ActivityConsumedBy'] == sector, 'FlowAmount'].values[0]
        pct = np.minimum(mecs_energy / ghgi_energy, 1)
        pct_dict[fuel] = pct

    return pct_dict


def allocate_industrial_combustion(df):
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_df_fxn. Calculate the percentage of fuel consumption captured in
    EIA MECS relative to EPA GHGI. Create new activities to distinguish those
    which use EIA MECS as allocation source and those that use alternate source.
    """
    # TODO make this year dynamic
    year = 2014
    pct_dict = get_manufacturing_energy_ratios(year)

    # activities reflect flows in A_14 and 3_8 and 3_9
    activities_to_split = {'Industrial Other Coal Industrial': 'Coal',
                           'Natural Gas  Industrial': 'Natural Gas',  # note extra space
                           'Coal Industrial': 'Coal',
                           'Natural gas industrial': 'Natural Gas'}

    for activity, fuel in activities_to_split.items():
        df_subset = df.loc[df['ActivityProducedBy'] == activity].reset_index(drop=True)
        if len(df_subset) == 0:
            continue
        df_subset['FlowAmount'] = df_subset['FlowAmount'] * pct_dict[fuel]
        df_subset['ActivityProducedBy'] = f"{activity} - Manufacturing"
        df.loc[df['ActivityProducedBy'] == activity,
               'FlowAmount'] = df['FlowAmount'] * (1-pct_dict[fuel])
        df = pd.concat([df, df_subset], ignore_index=True)

    return df


def split_HFCs_by_type(df):
    """Speciates HFCs and PFCs for all activities based on T_4_99."""
    splits = load_fba_w_standardized_units(datasource='EPA_GHGI_T_4_99',
                                           year=df['Year'][0])
    splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
    splits = splits[['FlowName', 'pct']]

    speciated_df = df.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
                            axis=1, result_type='expand')
    speciated_df.columns = splits['FlowName']
    speciated_df = pd.concat([df, speciated_df], axis=1)
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
    df_subset['FlowAmount'] = df_subset['FlowAmount'] * (household / (industry + household))
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
    df_subset['FlowAmount'] = df_subset['FlowAmount'] * (polystyrene / (urethane + polystyrene))
    df_subset['ActivityProducedBy'] = f"{activity} - Polystyrene"
    df.loc[df['ActivityProducedBy'] == activity,
           'FlowAmount'] = df['FlowAmount'] * (urethane / (urethane + polystyrene))
    df.loc[df['ActivityProducedBy'] == activity,
           'ActivityProducedBy'] = f"{activity} - Urethane"
    df = pd.concat([df, df_subset], ignore_index=True)

    return df


def clean_HFC_fba(df):
    """Adjust HFC emissions for improved parsing.
    clean_fba_before_mapping_df_fxn used in EPA_GHGI_T_4_101."""
    df = subtract_HFC_transport_emissions(df)
    df = allocate_HFC_to_residential(df)
    df = split_HFC_foams(df)
    df = split_HFCs_by_type(df)
    return df


def remove_HFC_kt(df):
    """Remove records of emissions in kt, data are also provided in MMT CO2e.
    clean_fba_before_mapping_df_fxn used in EPA_GHGI_T_4_50."""
    return df.loc[df['Unit'] != 'kt']


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
    fba = flowsa.getFlowByActivity('EPA_GHGI_T_4_101', 2016)
    df = clean_HFC_fba(fba)
