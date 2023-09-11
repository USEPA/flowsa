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
from flowsa.flowsa_log import log
from flowsa.settings import externaldatapath
from flowsa.schema import flow_by_activity_fields
from flowsa.flowbyactivity import FlowByActivity

SECTOR_DICT = {'Res.': 'Residential',
               'Comm.': 'Commercial',
               'Ind.': 'Industrial',
               'Trans.': 'Transportation',
               'Elec.': 'Electricity Power',
               'Terr.': 'U.S. Territory'}

ANNEX_HEADERS = {"Total Consumption (TBtu) a": "Total Consumption (TBtu)",
                 "Total Consumption (TBtu)a": "Total Consumption (TBtu)",
                 "Adjustments (TBtu) b": "Adjustments (TBtu)",
                 "Adjusted Consumption (TBtu) a": "Adjusted Consumption (TBtu)",
                 "Adjusted Consumption (TBtu)a": "Adjusted Consumption (TBtu)",
                 "Emissions b (MMT CO2 Eq.) from Energy Use":
                     "Emissions (MMT CO2 Eq.) from Energy Use",
                 "Emissionsb (MMT CO2 Eq.) from Energy Use":
                     "Emissions (MMT CO2 Eq.) from Energy Use",
                 }

# Tables for annual CO2 emissions from fossil fuel combustion
ANNEX_ENERGY_TABLES = ["A-" + str(x) for x in list(range(4,16))]

DROP_COLS = ["Unnamed: 0"] + list(pd.date_range(
    start="1990", end="2010", freq='Y').year.astype(str))

YEARS = list(pd.date_range(start="2010", end="2021", freq='Y').year.astype(str))


def ghg_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
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
    if table == "A-4": 
        # Table "Energy Consumption Data by Fuel Type (TBtu) and Adjusted 
        # Energy Consumption Data"
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
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        frames = []
        if any(x in url for x in ['annex', 'Annex']):
            is_annex = True
            t_tables = config['Annex']
        else:
            is_annex = False
            t_tables = config['Tables']
        for chapter, tables in t_tables.items():
            for table in tables:
                df = None
                tbl_year = tables[table].get('year')
                if tbl_year is not None and tbl_year != year:
                    # Skip tables when the year does not align with target year
                    continue

                table_name = tables[table].get('table_name', table)
                if is_annex:
                    path = config['path']['annex']
                else:
                    path = config['path']['base']
                path = (path.replace('{chapter}', chapter)
                            .replace('{table_name}', table_name))

                # Handle special case of table 3-22 in external data folder
                if table == "3-22b":
                    if str(year) in ['2020']:
                        # Skip 3-22b for current year (use 3-22 instead)
                        continue
                    else:
                        df = pd.read_csv(externaldatapath / f"GHGI_Table_{table}.csv",
                                         skiprows=2, encoding="ISO-8859-1", thousands=",")
                else:
                    try:
                        data = f.open(path)
                    except KeyError:
                        log.error(f"error reading {table}")
                        continue
                
                if table in ['3-10', '5-28', 'A-73', 'A-97']:
                    # Skip single row
                    df = pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                     thousands=",", decimal=".")
                    df = df.rename(columns={'2010a':'2010'})
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
                elif table != '3-22b':
                    # Except for 3-22b already as df, 
                    # Proceed with default case
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                     thousands=",")

                if table == '3-13':
                    # remove notes from column headers in some years
                    cols = [c[:4] for c in list(df.columns[1:])]
                    df = df.rename(columns=dict(zip(df.columns[1:], cols)))

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
    if ('consum' in get_table_meta(source_name, config)['desc'].lower() and
        get_table_meta(source_name, config)['class']!='Chemicals'):
        return True
    return False

def strip_char(text):
    """
    Removes the footnote chars from the text
    """
    text = text + " "
    notes = ["f, g", " a ", " b ", " c ", " d ", " e ", " f ", " g ",
             " h ", " i ", " j ", " k ", " l ", " b,c ", " h,i ", " f,g ",
             ")b", ")f", ")k", "b,c", "h,i"]
    for i in notes:
        if i in text:
            text_split = text.split(i)
            text = text_split[0]

    footnotes = {'Gasolineb': 'Gasoline',
                 'Trucksc': 'Trucks',
                 'Boatsd': 'Boats',
                 'Boatse': 'Boats',
                 'Fuelsb': 'Fuels',
                 'Fuelsf': 'Fuels',
                 'Consumptiona': 'Consumption',
                 'Aircraftg': 'Aircraft',
                 'Pipelineh': 'Pipeline',
                 'Electricityh': 'Electricity',
                 'Electricityl': 'Electricity',
                 'Ethanoli': 'Ethanol',
                 'Biodieseli': 'Biodiesel',
                 'Changee': 'Change',
                 'Emissionsc': 'Emissions',
                 'Equipmentd': 'Equipment',
                 'Equipmente': 'Equipment',
                 'Totalf': 'Total',
                 'Roadg': 'Road',
                 'Otherf': 'Other',
                 'Railc': 'Rail',
                 'Usesb': 'Uses',
                 'Substancesd': 'Substances',
                 'Territoriesa': 'Territories',
                 'Roadb': 'Road',
                 'Raile': 'Rail',
                 'LPGf': 'LPG',
                 'Gasf': 'Gas',
                 'Gasolinec': 'Gasoline',
                 'Gasolinef': 'Gasoline',
                 'Fuelf': 'Fuel',
                 'Amendmenta': 'Amendment',
                 'Residue Nb': 'Residue N',
                 'Residue Nd': 'Residue N',
                 'Landa': 'Land',
                 'Landb': 'Land',
                 'landb': 'land',
                 'landc': 'land',
                 'landd': 'land',
                 'Settlementsc': 'Settlements',
                 'Wetlandse': 'Wetlands',
                 'Settlementsf': 'Settlements',
                 'Totali': 'Total',
                 'Othersa': 'Others',
                 'N?O': 'N2O',
                 'Distillate Fuel Oil (Diesel': 'Distillate Fuel Oil',
                 'Natural gas': 'Natural Gas', # Fix capitalization inconsistency
                 'N2O (Semiconductors)': 'N2O',
                 'HGLb': 'HGL',
                 }
    for key in footnotes:
        text = text.replace(key, footnotes[key])

    return ' '.join(text.split()) # remove extra spaces between words



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
        df["FlowAmount"].replace(" NE ", np.nan, inplace=True)
        df["FlowAmount"].replace("NE", np.nan, inplace=True)
        # Convert all empty cells to nan cells
        df["FlowAmount"].replace("", np.nan, inplace=True)
        # Table 3-10 has some NO (Not Occuring) values, dropping these.
        df["FlowAmount"].replace(" NO ", np.nan, inplace=True)
        df["FlowAmount"].replace("NO", np.nan, inplace=True)
        # Table A-118 has some IE values, dropping these.
        df["FlowAmount"].replace("IE", np.nan, inplace=True)
        df["FlowAmount"].replace(r'NO ', np.nan, inplace=True)

        # Drop any nan rows
        df.dropna(subset=['FlowAmount'], inplace=True)

        if table_name not in ANNEX_ENERGY_TABLES:
            if 'Unit' not in df:
                df['Unit'] = meta.get("unit")
            if 'FlowName' not in df:
                df['FlowName'] = meta.get('flow')

            df["Class"] = meta.get("class")
            df["Description"] = meta.get("desc")
            df["Compartment"] = meta.get("compartment")

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

        # Define special table lists from config      
        multi_chem_names = config.get('multi_chem_names')
        source_No_activity = config.get('source_No_activity')
        source_activity_1 = config.get('source_activity_1')
        source_activity_1_fuel = config.get('source_activity_1_fuel')
        source_activity_2 = config.get('source_activity_2')

        if table_name in multi_chem_names:
            bool_apb = False
            bool_LULUCF = False
            apbe_value = ""
            flow_name_list = ["CO2", "CH4", "N2O", "NF3", "HFCs", "PFCs",
                              "SF6", "NF3", "CH4 a", "N2O b", "CO", "NOx"]
            for index, row in df.iterrows():
                apb_value = strip_char(row["ActivityProducedBy"])
                if "CH4" in apb_value:
                    apb_value = "CH4"
                elif "N2O" in apb_value and apb_value != "N2O from Product Uses":
                    apb_value = "N2O"
                elif "CO2" in apb_value:
                    apb_value = "CO2"

                if apb_value in flow_name_list:
                    if bool_LULUCF:
                        df = df.drop(index)
                    else:
                        apbe_value = apb_value
                        df.loc[index, 'FlowName'] = apbe_value
                        df.loc[index, 'ActivityProducedBy'] = "All activities"
                        bool_apb = True
                elif apb_value.startswith('LULUCF'):
                    df.loc[index, 'FlowName'] = 'CO2e'
                    df.loc[index, 'ActivityProducedBy'] = strip_char(apb_value)
                    bool_LULUCF = True
                elif apb_value.startswith(('Total', 'Net')):
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

        elif table_name in (source_activity_1 + source_activity_1_fuel) :
            apbe_value = ""
            activity_subtotal_sector = ["Electric Power", "Industrial", "Commercial",
                                 "Residential", "U.S. Territories",
                                 "Transportation",
                                 "Exploration",
                                 "Production (Total)", "Refining",
                                 "Crude Oil Transportation",
                                 "Cropland", "Grassland"]
            activity_subtotal_fuel = [
                "Gasoline", "Distillate Fuel Oil",
                "Jet Fuel", "Aviation Gasoline", "Residual Fuel Oil",
                "Natural Gas", "LPG", "Electricity",
                "Fuel Type/Vehicle Type", "Diesel On-Road",
                "Alternative Fuel On-Road", "Non-Road",
                "Gasoline On-Road", "Distillate Fuel Oil",
                "Biofuels-Ethanol", "Biofuels-Biodiesel",
                ]
            if table_name in source_activity_1:
                activity_subtotal = activity_subtotal_sector
            else:
                activity_subtotal = activity_subtotal_fuel
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

        elif table_name == "A-73":
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
                    fuel_name = strip_char(fuel_name.split('(')[0])
                    df.loc[index, 'ActivityConsumedBy'] = "All activities"
                    df.loc[index, 'FlowName'] = fuel_name
                if fuel_name in A_79_unit_dict.keys():
                    df.loc[index, 'Unit'] = A_79_unit_dict[fuel_name]

        else:
            if table_name in ["4-48"]:
                # Assign activity as flow for technosphere flows
                df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']

            elif table_name in ["4-86", "4-96", "4-100"]:
                # Table with flow names as Rows
                df.loc[:, 'FlowName'] = (df.loc[:, 'ActivityProducedBy']
                                         .apply(lambda x: strip_char(x)))
                df = df[~df['FlowName'].str.contains("Total")]
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')

            elif table_name in ["4-33", "4-50", "4-80"]:
                # Table with units or flows as columns
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')
                df.loc[df['Unit'] == 'MMT CO2 Eq.', 'Unit'] = 'MMT CO2e'
                df.loc[df['Unit'].str.contains('kt'), 'Unit'] = 'kt'

            elif table_name in ["4-14", "4-102", "A-95"]:
                # Remove notes from activity names
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"].split("(")[0])
                    df.loc[index, 'ActivityProducedBy'] = apb_value

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


def get_manufacturing_energy_ratios(parameter_dict):
    """Calculate energy ratio by fuel between GHGI and EIA MECS."""
    # flow correspondence between GHGI and MECS
    flow_corr = {'Industrial Other Coal': 'Coal',
                 'Natural Gas': 'Natural Gas',
                 # 'Total Petroleum': (
                 #     'Petroleum', ['Residual Fuel Oil',
                 #                   'Distillate Fuel Oil',
                 #                   'Hydrocarbon Gas Liquids, excluding natural gasoline',
                 #                   ])
                 }
    mecs_year = parameter_dict.get('year')

    # Filter MECS for total national energy consumption for manufacturing sectors
    mecs = load_fba_w_standardized_units(datasource=parameter_dict.get('energy_fba'),
                                         year=mecs_year,
                                         flowclass='Energy',
                                         download_FBA_if_missing=True)
    mecs = (mecs.loc[(mecs['ActivityConsumedBy'] == '31-33') &
                     (mecs['Location'] == '00000') &
                     (mecs['Description'].isin(['Table 3.2', 'Table 2.2'])) &
                     (mecs['Unit'] == 'MJ')]
            .reset_index(drop=True))

    # Load energy consumption data by fuel from GHGI
    ghgi = load_fba_w_standardized_units(datasource=parameter_dict.get('ghg_fba'),
                                         year=mecs_year,
                                         flowclass='Energy',
                                         download_FBA_if_missing=True)
    ghgi = ghgi[ghgi['ActivityConsumedBy']=='Industrial'].reset_index(drop=True)

    pct_dict = {}
    for ghgi_flow, v in flow_corr.items():
        if type(v) is tuple:
            label = v[0]
            mecs_flows = v[1]
        else:
            label = v
            mecs_flows = [v]
        # Calculate percent energy contribution from MECS based on v
        mecs_energy = sum(mecs.loc[mecs['FlowName'].isin(mecs_flows), 'FlowAmount'].values)
        ghgi_energy = ghgi.loc[ghgi['FlowName'] == ghgi_flow, 'FlowAmount'].values[0]
        pct = np.minimum(mecs_energy / ghgi_energy, 1)
        pct_dict[label] = pct

    return pct_dict


def allocate_industrial_combustion(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_df_fxn. Calculate the percentage of fuel consumption captured in
    EIA MECS relative to EPA GHGI. Create new activities to distinguish those
    which use EIA MECS as allocation source and those that use alternate source.
    """
    pct_dict = get_manufacturing_energy_ratios(fba.config.get('clean_parameter'))

    # activities reflect flows in A_14 and 3_8 and 3_9
    activities_to_split = {'Industrial Other Coal Industrial': 'Coal',
                           'Natural Gas Industrial': 'Natural Gas',
                           'Coal Industrial': 'Coal',
                           # 'Total Petroleum Industrial': 'Petroleum',
                           # 'Fuel Oil Industrial': 'Petroleum',
                           }

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


def split_HFCs_by_type(fba: FlowByActivity, **_) -> FlowByActivity:
    """Speciates HFCs and PFCs for all activities based on T_4_100.
    clean_fba_before_mapping_df_fxn"""

    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    original_sum = fba.FlowAmount.sum()
    tbl = fba.config.get('clean_parameter')['flow_fba'] # 4-100
    splits = load_fba_w_standardized_units(datasource=tbl,
                                           year=fba['Year'][0],
                                           download_FBA_if_missing=True)
    splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
    splits = splits[['FlowName', 'pct']]

    speciated_df = fba.apply(lambda x: [p * x['FlowAmount'] for p in splits['pct']],
                             axis=1, result_type='expand')
    speciated_df.columns = splits['FlowName']
    fba = pd.concat([fba, speciated_df], axis=1)
    fba = (fba
           .melt(id_vars=[c for c in flow_by_activity_fields.keys() if c in fba],
                 var_name='Flow')
           .drop(columns=['FlowName', 'FlowAmount'])
           .rename(columns={'Flow': 'FlowName',
                            'value': 'FlowAmount'}))
    new_sum = fba.FlowAmount.sum()
    if round(new_sum, 6) != round(original_sum, 6):
        log.warning('Error: totals do not match when splitting HFCs')
    new_fba = FlowByActivity(fba)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba


if __name__ == "__main__":
    import flowsa
    # fba = flowsa.getFlowByActivity('EPA_GHGI_T_4_101', 2016)
    # df = clean_HFC_fba(fba)
    fba = flowsa.generateflowbyactivity.main(year=2017, source='EPA_GHGI')
