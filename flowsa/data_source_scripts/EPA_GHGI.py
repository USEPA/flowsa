# EPA_GHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks
"""

import io
import re
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
    start="1990", end="2010", freq='YE').year.astype(str))

YEARS = list(pd.date_range(start="2010", end="2024", freq='YE').year.astype(str))


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
    main_url = f"{build_url}{config['url'].get('main_zip')}"
    annex_url = f"{build_url}{config['url'].get('annex_zip')}"
    return [main_url, annex_url]


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
    dropcols = []
    for i in range(len(df.columns)):
        fuel_type = str(df.iloc[0, i])
        for abbrev, full_name in SECTOR_DICT.items():
            fuel_type = fuel_type.replace(abbrev, full_name)
        fuel_type = fuel_type.strip()

        col_name = df.columns[i][1]
        if df.iloc[:, i].isnull().all():
            # skip over mis aligned columns
            dropcols.append(i)
            continue
        if "Unnamed" in col_name:
            column_name = header_name
        elif col_name in ANNEX_HEADERS.keys():
            column_name = ANNEX_HEADERS[col_name]
            header_name = ANNEX_HEADERS[col_name]
        else:
        # Fallback assignment if col_name is not in ANNEX_HEADERS
            column_name = col_name.strip()
            header_name = column_name  # update the header_name for potential Unnamed columns later

        newcols.append(f"{column_name} - {fuel_type}")
        if "Unnamed" not in col_name and col_name not in ANNEX_HEADERS:
            log.warning(f"Unknown column header encountered: {col_name}")
            
    df = df.drop(columns=df.columns[dropcols])
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
            opath = config['path']['annex']
            t_tables = config['Annex']
            annex = True
        else:
            opath = config['path']['base']
            t_tables = config['Tables']
            annex = False
        for chapter, tables in t_tables.items():
            if annex:
            # Annex tables are in separate folders
                for table in tables:
                    # print(table)
                    df = None
                    tbl_year = tables[table].get('year')
                    if tbl_year is not None and tbl_year != year:
                        # Skip tables when the year does not align with target year
                        continue
                    table_name = tables[table].get('table_name', table)
                    path = (opath.replace('{table_name}', table_name)
                                 .replace('{annex}', chapter))
                    try:
                        data=f.open(path)
                    except KeyError:
                        log.error(f"error reading {table}")
                        continue
                    if table in ANNEX_ENERGY_TABLES:
                        df=annex_yearly_tables(data, table)
                    else:
                        df=pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                       thousands=",")

                    if df is not None and len(df.columns) > 1:
                        years=YEARS.copy()
                        years.remove(str(year))
                        df=df.drop(columns=(DROP_COLS + years), errors='ignore')
                        df["SourceName"]=f"EPA_GHGI_T_{table.replace('-', '_')}"
                        frames.append(df)
                    else:
                        log.warning(f"Error accessing {table}")

            else:
            # Access chapter specific folders within the main zip
                for table in tables:
                    # print(table)
                    df = None
                    tbl_year = tables[table].get('year')
                    if tbl_year is not None and tbl_year != year:
                        # Skip tables when the year does not align with target year
                        continue
                    table_name = tables[table].get('table_name', table)
                    path = f"{chapter}/{opath.replace('{table_name}', table_name)}"
                    # Handle special case of table 3-25 in external data folder
                    if table == "3-25b":
                        if str(year) in ['2023']:
                            # Skip 3-25b for current year (use 3-25 instead)
                            continue
                        else:
                            df=pd.read_csv(externaldatapath / f"GHGI_Table_{table}.csv",
                                             skiprows=2, encoding="ISO-8859-1", thousands=",")
                    else:
                        try:
                            data=f.open(path)
                        except KeyError:
                            log.error(f"error reading {table}")
                            continue
                    if table in ['4-118']:
                        # Skip two rows
                        df=pd.read_csv(data, skiprows=2, encoding="ISO-8859-1",
                                         thousands=",", decimal=".")
                    elif table == "3-25":
                        # Skip first row, but make headers the next 2 rows:
                        df=pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                         header=[0, 1], thousands=",")
                        # Row 0 is header, row 1 is unit
                        new_headers=[]
                        for col in df.columns:
                            new_header='Unnamed: 0'
                            if 'Unnamed' not in col[0]:
                                if 'Unnamed' not in col[1]:
                                    new_header=f'{col[0]} {col[1]}'
                                else:
                                    new_header=col[0]
                            else:
                                new_header=col[1]
                            new_headers.append(new_header)
                        df.columns=new_headers
                    elif table != '3-25b':
                        # Except for 3-25b already as df,
                        # Proceed with default case
                        df=pd.read_csv(data, skiprows=1, encoding="ISO-8859-1",
                                         thousands=",")

                    if table == '3-13':
                        # remove notes from column headers in some years
                        cols=[c[:4] for c in list(df.columns[1:])]
                        df=df.rename(columns=dict(zip(df.columns[1:], cols)))

                    if df is not None and len(df.columns) > 1:
                        years=YEARS.copy()
                        years.remove(str(year))
                        df=df.drop(columns=(DROP_COLS + years), errors='ignore')
                        df["SourceName"]=f"EPA_GHGI_T_{table.replace('-', '_')}"
                        frames.append(df)
                    else:
                        log.warning(f"Error accessing {table}")
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
             ")a", ")b", ")f", ")k", "b,c", "h,i"]
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
                 'Distillate Fuel Oil (Diesel)': 'Distillate Fuel Oil',
                 'Distillate Fuel Oil (Diesel': 'Distillate Fuel Oil',
                 'Natural gas': 'Natural Gas', # Fix capitalization inconsistency
                 'HGLb': 'HGL',
                 'Biofuels-Biodieselh' : 'Biofuels-Biodiesel',
                 'Biofuels-Ethanolh' : 'Biofuels-Ethanol',
                 'Commercial Aircraftf' : 'Commercial Aircraft',
                 'Electricityk': 'Electricity',
                 'Gasolinea' : 'Gasoline',
                 'International Bunker Fuelse' : 'International Bunker Fuel',
                 'Medium- and Heavy-Duty Trucksb' : 'Medium- and Heavy-Duty Trucks',
                 'Pipelineg' : 'Pipeline',
                 'Recreational Boatsc' :'Recreational Boats',
                 'Construction/Mining Equipmentf' : 'Construction/Mining Equipment',
                 'Non-Roadc' : 'Non-Road',
                 'HFCsa': 'HFCs',
                 'HFOsb': 'HFOs',
                 'CO_{2}': 'CO2',
                 'CH?^{c}': 'CH4',
                 'N_{2} O^{c}': 'N2O',
                 'N_{2} O': 'N2O',
                 'SF?': 'SF6',
                 'NF?': 'NF3',
                 'CH_{4}': 'CH4',
                 'Total e,j': 'Total',
                 'Naphtha (<401Â° F)': 'Naphtha (<401° F)',
                 'Other Oil (>401Â° F)': 'Other Oil (>401° F)',
                 }
    text = re.sub(r"\^\{[a-zA-Z]\}", "", text)

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

        if table_name in ['3-25']:
            df = df.melt(id_vars=id_vars,
                         var_name=meta.get('melt_var'),
                         value_name="FlowAmount")
            name_unit = series_separate_name_and_units(df['FlowName'],
                                                       meta['activity'],
                                                       meta['unit'])
            df['FlowName'] = name_unit['names']
            df['Unit'] = name_unit['units']
            df['Year'] = year

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

        # set suppressed values to 0 but mark as suppressed
        # otherwise set non-numeric to nan
        try:
            df['Suppressed'] = (df['FlowAmount']
                                .astype(str).str.strip().eq('+')
                                .replace({True: '+', False: np.nan})
                                .infer_objects(copy=False)
                                )
            df['FlowAmount'] = df['FlowAmount'].astype(str).str.replace(',', '').infer_objects(copy=False)
            df['FlowAmount'] = df['FlowAmount'].replace('+', '0').infer_objects(copy=False)
            df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce')
            df = df.dropna(subset='FlowAmount')
        except AttributeError:
            # if no string in FlowAmount, then proceed
            df = df.dropna(subset='FlowAmount')

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
        df["DataReliability"] = meta.get("data_reliability", 5)
        df["DataCollection"] = 1
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
        rows_as_flows = config.get('rows_as_flows')

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
                ]
            if table_name in source_activity_1:
                activity_subtotal = activity_subtotal_sector
            else:
                activity_subtotal = activity_subtotal_fuel
            after_Total = False
            for index, row in df.iterrows():
                apb_value = strip_char(row["ActivityProducedBy"])
                if apb_value in activity_subtotal or after_Total:
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
                    after_Total = True

        elif table_name in source_activity_2:
            bool_apb = False
            apbe_value = ""
            flow_name_list = ["Explorationb", "Production", "Processing",
                              "Transmission and Storage", "Distribution",
                              "Post-Meter",
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

        elif table_name == "A-69":
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
            if table_name in ["4-55"]:
                # Assign activity as flow for technosphere flows
                df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']

            elif table_name in ["4-118", "4-132"]:
                df = df.iloc[::-1] # reverse the order for assigning APB
                for index, row in df.iterrows():
                    apb_value = strip_char(row["ActivityProducedBy"])
                    if apb_value.startswith('Total'):
                        # set the header
                        apbe_value = apb_value.replace('Total ','')
                        df = df.drop(index)
                    else:
                        if apbe_value == 'N2O':
                            df.loc[index, 'ActivityProducedBy'] = (
                                re.findall(r'\(.*?\)', apb_value)[0][1:-1])
                            df.loc[index, 'FlowName'] = 'N2O'
                        else:
                            df.loc[index, 'ActivityProducedBy'] = apbe_value
                            df.loc[index, 'FlowName'] = apb_value
                df = df.iloc[::-1] # revert the order

            elif table_name in rows_as_flows:
                # Table with flow names as Rows
                df.loc[:, 'FlowName'] = (df.loc[:, 'ActivityProducedBy']
                                         .apply(lambda x: strip_char(x)))
                df = df[~df['FlowName'].str.contains("Total")]
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')

            elif table_name in ["4-16", "4-124"]:
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
        df["FlowAmount"] = df["FlowAmount"].replace(',', '', regex=True)
        if len(df) == 0:
            log.warning(f"Error processing {table_name}")
        else:
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

    clean_fba_before_activity_sets. Calculate the percentage of fuel consumption captured in
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
    """Speciates HFCs and PFCs for all activities based on T_4_125.
    clean_fba_before_mapping_df_fxn"""

    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    original_sum = fba.FlowAmount.sum()
    tbl = fba.config.get('clean_parameter')['flow_fba'] # 4-125
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
    # fba = flowsa.return_FBA('EPA_GHGI_T_4_101', 2016)
    # df = clean_HFC_fba(fba)
    tbl_list = ["2-1", "3-7", "3-8","3-9","3-13","3-14","3-15",#"3-25","3-25b",
                "3-106","3-45","3-47","3-49","3-64","3-66","3-68","3-102",
                "4-16","4-39","4-59","4-100","4-55","4-57","4-63","4-64","4-106", "4-118",
                "4-122","4-124","4-132",
                "5-3","5-7","5-18","5-19","5-29",
                # "A-5"
                ]
    fba_list = []
    for y in range(2019, 2024):
        flowsa.generateflowbyactivity.main(year=y, source='EPA_GHGI')
        if y == 2023:
            ls = tbl_list + ['3-25', 'A-5']
        else:
            ls = tbl_list + ['3-25b'] + [f'A-{2028-y}']
        fba = pd.concat([flowsa.getFlowByActivity(f'EPA_GHGI_T_{str(t).replace("-","_")}', y)
                         for t in ls])
        fba_list.append(fba)
    fba_all = pd.concat(fba_list, ignore_index=True)
