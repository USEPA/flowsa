# EPA_WFR.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from 2018 Wasted Food Report.
"""

import io
import pandas as pd
import numpy as np
from string import ascii_uppercase
from tabula.io import read_pdf
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS
from flowsa.flowbyactivity import FlowByActivity


def epa_wfr_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    result_list = []
    df_list = []
    result = pd.DataFrame()
    df = pd.DataFrame()
    pages = range(41, 43)
    for x in pages:
        df_l = read_pdf(io.BytesIO(resp.content),
                        pages=x, stream=True)
        if len(df_l[0].columns) == 12:
            df = df_l[0].set_axis(
                ['Management Pathway', 'Manufacturing/Processing',
                 'Residential', 'Retail', 'Wholesale', 'Hotels', 'seven',
                 'eight', 'K-12 Schools',  'Food Banks',
                 'Intermediate Amount Managed',
                 'Total Managed by Each Pathway'],
                axis=1)
        else:
            df = df_l[0].set_axis(
                ['Management Pathway', 'Manufacturing/Processing',
                 'Residential', 'Retail', 'Wholesale', 'Hotels',
                 'seven', 'K-12 Schools',  'Food Banks',
                 'Intermediate Amount Managed',
                 'Total Managed by Each Pathway'],
                axis=1)
        df = drop_rows(df)
        df_list.append(df)
    for d in df_list:
        result = pd.concat([result, d])
    result = fix_row_names(result)
    result = split_problem_column(result)
    result = reorder_df(result)
    result_list.append(result)
    return result_list


def epa_wfr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)

    # hardcode
    # original data in short tons
    df['Class'] = 'Other'
    df['SourceName'] = 'EPA_WFR'
    df['FlowName'] = 'Food Waste'
    df['FlowType'] = 'WASTE_FLOW'
    df['Compartment '] = 'None'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = year
    df['Unit'] = 'short tons'
    df['Description'] = 'Excess food and food waste managed by sector (Tons)'
    return df


def drop_rows(df):
    """
    This drops the column headers for the table which ended up being scraped as
    4 rows. We are also dropping all rows with null values under the
    MANUFACTURING/ PROCESSING section. When the PDF was scraped additional
    rows were added due to how the MANUFACTURING/ PROCESSING column was
    formatted. Since - was used instead of NaN in the PDF to indicate
    something did not exist NaN values could be used to get rid of
    artificial rows as they were the only ones with NaN values in them.
    :param df: dataframe
    :return: df
    """
    df = df.drop(index=[0, 1, 2, 3])
    df = df.dropna(axis=0, subset=['Manufacturing/Processing'])
    return df


def fix_row_names(df):
    """
    The Scrape from the PDF caused some of the names to end up in more than
    1 row. This also caused artificial rows which have been dealt with in
    another method. The dataframe is in order so a new list is added to the
    dataframe it is ActivityConsumedBy. We then reindex the dataframe and
    drop the last two columns. They are total columns and are not wanted in
    the parquet file.
    :param df: dataframe
    :return: df
    """
    acb = ['Food Donation', 'Animal Feed', 'Codigestion/Anaerobic Digestion',
           'Composting/Aerobic Processes',
           'Bio-based Materials/Biochemical Processing', 'Land Application',
           'Sewer/Wastewater Treatment', 'Landfill',
           'Controlled Combustion', 'Total Food Waste & Excess Food',
           'Percent of Total']
    df['ActivityConsumedBy'] = acb
    df = df.reset_index()
    df = df.drop(columns=['index', 'Management Pathway'])
    df = df.drop(index=[9, 10])
    return df


def split_problem_column(df):
    """
     When the table was scraped from the PDF the 7th and 8th columns were
     not correct. Depending on the page the table was printed on the seventh
     column either 7 or 8 data points in it. This method takes the 7th and
     8th column and splits them into lists and then adds them to the
     dataframe. Additionally the Scrape for the PDF caused letters to be in
     some of the numbers. They are removed. Also at one point a number gets
     split into 2 columns that is also taken care of here.
    :param df: dataframe
    :return: df
     """
    t = str.maketrans('', '', ascii_uppercase)
    seven_array_corrected = []
    restaurants_list = []
    sports_list = []
    hospitals_list = []
    nursing_list = []
    military_list = []
    office_list = []
    correctional_list = []
    colleges_list = []
    index = 0


    col_seven_list = df['seven'].tolist()
    col_eight_list = df['eight'].tolist()
    for i in col_seven_list:
        strip = i.translate(t)
        seven_array = strip.split(" ")
        seven_array = ' '.join(seven_array).split()
        if len(seven_array) == 8:
            for idx, val in enumerate(seven_array):
                seven_array[idx] = val
                if val[0] == ",":
                    value_str = seven_array[idx - 1]
                    value_str = value_str + val
                    seven_array[idx - 1] = value_str
                    seven_array[idx] = ""
            seven_array = ' '.join(seven_array).split()

        restaurants_list.append(seven_array[0])
        sports_list.append(seven_array[1])
        hospitals_list.append(seven_array[2])
        nursing_list.append(seven_array[3])
        military_list.append(seven_array[4])
        office_list.append(seven_array[5])
        correctional_list.append(seven_array[6])

        if len(seven_array) == 7:
            colleges_list.append(col_eight_list[index])
        else:
            colleges_list.append(seven_array[7])

        index = index + 1
    df['Restaurants/Food Services'] = restaurants_list
    df['Sports Venues'] = sports_list
    df['Hospitals'] = hospitals_list
    df['Nursing Homes'] = nursing_list
    df['Military Installations'] = military_list
    df['Office Buildings'] = office_list
    df['Correctional Facilities'] = correctional_list
    df['Colleges & Universities'] = colleges_list
    df = df.drop(columns=['seven', 'eight', 'Intermediate Amount Managed',
                 'Total Managed by Each Pathway'])

    return df


def reorder_df(df):
    """
    Melts the pandas dataframe into flow by activity format.
    Drops any rows where flow amount is -
    Gets rid of the commas in the flow amount.
    Resets the index of the dataframe and returns the dataframe
    :param df: dataframe
    :return: df
    """
    df = df.melt(id_vars="ActivityConsumedBy",
                 var_name="ActivityProducedBy",
                 value_name="FlowAmount")
    indexResult = df[df['FlowAmount'] == '-'].index
    df = df.replace(',', '', regex=True)
    df.drop(indexResult, inplace=True)
    df = df.reset_index()
    df = df.drop(columns=['index'])
    return df


def return_REI_fraction_foodwaste_treated_commodities():
    """
    Return dictionary of how the food waste is used after entering
    waste management pathways - fractions are pulled from EPA REI
    https://www.epa.gov/smm/recycling-economic-information-rei-report
    :return: dict, food waste pathway food use
    """
    pathway_attribution = {
        'Animal Feed':  # Fresh wheat, corn, (1111B0)
            {'Fresh wheat, corn': 1},
        'Animal meal, meat, fats, oils, and tallow':
            {'Dog and cat food manufacturing': 0.31,
             'Other animal food manufacturing': 0.54,
             'Petrochemical manufacturing': 0.03,
             'Other basic organic chemical manufacturing': 0.03,
             'Soap and cleaning compound manufacturing': 0.03,
             'Toilet preparation manufacturing': 0.03,
             'Printing ink manufacturing': 0.03},
        'Biodiesel':
            {'Gasoline': 1},  # 324110
        'Anaerobic Digestion':
            {'Natural gas': 0.0469},  # 221200, On a mass basis, there is 0.0469 kg biogas per kg waste
        'Compost':
            {'Support activities for agriculture and forestry': 0.13,
             'Stone mining and quarrying': 0.02,
             'Other nonresidential structures': 0.02,
             'Pesticide and other agricultural chemical manufacturing': 0.8,
             'Wholesale Trade': 0.01,
             'Services to buildings and dwellings': 0.01,
             'Museums, historical sites, zoos, and parks': 0.01
             }
    }
    return pathway_attribution


def foodwaste_use(fba: FlowByActivity) -> FlowByActivity:
    """
    clean_fba_before_activity_sets

    Attribute food waste to how waste is used
    :param fba:
    :param source_dict:
    :return:
    """
    use = fba.config.get('activity_parameters')
    outputs = fba.loc[fba['ActivityConsumedBy'].isin(use)].reset_index(drop=True)
    outputs['ActivityProducedBy'] = outputs['ActivityConsumedBy']
    outputs = outputs.drop(columns='ActivityConsumedBy')
    outputs2 = outputs.aggregate_flowby()

    # load fw treatment dictionary
    fw_tmt = return_REI_fraction_foodwaste_treated_commodities()
    replace_keys = {'Animal meal, meat, fats, oils, and tallow': 'Bio-based Materials/Biochemical Processing',
                    'Anaerobic Digestion': 'Codigestion/Anaerobic Digestion',
                    'Compost': 'Composting/Aerobic Processes'}
    for k, v in replace_keys.items():
        fw_tmt[v] = fw_tmt.pop(k)

    fw_tmt = (pd.DataFrame(fw_tmt).rename_axis(index='ActivityConsumedBy', columns='ActivityProducedBy')
              .stack()
              .rename('Multiplier')
              .reset_index())
    outputs3 = outputs2.merge(fw_tmt, how='left')

    outputs3['FlowName'] = outputs3['FlowName'].apply(lambda x:
                                                      f"{x} Treated")
    # update flowamount with multiplier fractions
    outputs3['FlowAmount'] = outputs3['FlowAmount'] * outputs3['Multiplier']
    outputs3 = outputs3.drop(columns='Multiplier')

    # also in wasted food report - APB "food banks" are the output from the ACB "Food Donation"
    fba['FlowName'] = np.where(fba['ActivityProducedBy'] == 'Food Banks',
                               fba["FlowName"].apply(
                                   lambda x: f"{x} Treated"),
                               fba["FlowName"])

    df1 = pd.concat([fba, outputs3], ignore_index=True)
    df2 = df1.aggregate_flowby()

    return df2

def reset_wfr_APB(fba, **_):
    """
    For "Waste_national_2018", only interested in total food waste that
    enters a waste management pathway, not interested in where the food
    waste is generated. Remove the activity produced by values to enable
    direct attribution of food waste rather than requiring attribution data
    sources for the waste generation
    :return:
    """

    fba['ActivityProducedBy'] = None

    return fba
