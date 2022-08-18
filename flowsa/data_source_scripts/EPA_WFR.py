# EPA_WFR.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from 2018 Wasted Food Report.
"""

import io
import pandas as pd
from string import ascii_uppercase
import tabula
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
from flowsa.location import US_FIPS
from flowsa.schema import flow_by_activity_mapped_fields


def epa_wfr_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    result_list = []
    df_list = []
    result = pd.DataFrame()
    df = pd.DataFrame()
    pages = range(41, 43)
    for x in pages:
        df_l = tabula.read_pdf(io.BytesIO(resp.content),
                             pages=x, stream=True)
        if len(df_l[0].columns) == 12:
            df = df_l[0].set_axis(
                ['Management Pathway', 'Manufacturing/Processing',
                 'Residential', 'Retail', 'Wholesale', 'Hotels', 'seven',
                 'eight', 'K-12 Schools',  'Food Banks',
                 'Intermediate Amount Managed',
                 'Total Managed by Each Pathway'],
                axis=1, inplace=False)
        else:
            df = df_l[0].set_axis(
                ['Management Pathway', 'Manufacturing/Processing',
                 'Residential', 'Retail', 'Wholesale', 'Hotels',
                 'seven', 'K-12 Schools',  'Food Banks',
                 'Intermediate Amount Managed',
                 'Total Managed by Each Pathway'],
                axis=1, inplace=False)
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


def attribute_cnhw_food(flows, method, k, v, *_):
    """
    TODO: incorporate this method into flowbysector.py after brining in
     Matthew's changes - cnhw is loaded as the primary data source,
     flowbysector.py is not set up for an FBS to be used as a primary data
     source

    Function is used to attribute CNHW food generation to waste management
    paths using EPA WFR and Facts and Figures
    :param flows:
    :param method:
    :return:
    """
    from flowsa.fbs_allocation import load_map_clean_fba
    from flowsa.settings import vLog, log
    from flowsa.validation import compare_activity_to_sector_flowamounts, \
        compare_fba_geo_subset_and_fbs_output_totals

    # empty list for activity results
    activity_list = []
    activities = v['activity_sets']
    # subset activity data and allocate to sector
    for aset, attr in activities.items():
        # subset by named activities
        names = attr['names']
        vLog.info(f"Preparing to handle {aset} in {k}")
        # subset fba data by activity
        flows_subset = flows[flows['SectorProducedBy'].isin(
            names)].reset_index(drop=True)

        # load allocation df
        # add parameters to dictionary if exist in method yaml
        fba_dict = {}
        if 'allocation_flow' in attr:
            fba_dict['flowname_subset'] = attr['allocation_flow']
        if 'allocation_compartment' in attr:
            fba_dict['compartment_subset'] = attr['allocation_compartment']
        if 'clean_allocation_fba' in attr:
            fba_dict['clean_fba'] = attr['clean_allocation_fba']
        if 'clean_allocation_fba_w_sec' in attr:
            fba_dict['clean_fba_w_sec'] = attr['clean_allocation_fba_w_sec']

        # load the allocation FBA
        fba_allocation_wsec = \
            load_map_clean_fba(method, attr,
                               fba_sourcename=attr['allocation_source'],
                               df_year=attr['allocation_source_year'],
                               flowclass=attr['allocation_source_class'],
                               geoscale_from=attr['allocation_from_scale'],
                               geoscale_to=v['geoscale_to_use'],
                               download_FBA_if_missing=True,
                               **fba_dict)

        # subset fba datasets to only keep the sectors associated
        # with activity subset
        if aset == 'wasted_food_report':
            log.info("Subsetting %s for sectors in %s",
                     attr['allocation_source'], k)
            fba_allocation_subset = fba_allocation_wsec[fba_allocation_wsec[
                'SectorProducedBy'].isin(names)].reset_index(drop=True)

            # determine ratios of food waste by waste management pathway
            fba_allocation_subset = fba_allocation_subset.assign(
                Denominator=fba_allocation_subset.groupby(
                    ['SectorProducedBy'])['FlowAmount'].transform('sum'))
            fba_allocation_subset = fba_allocation_subset.assign(
                FlowAmountRatio=fba_allocation_subset['FlowAmount'] /
                                fba_allocation_subset['Denominator'])

            # merge the primary data source and allocation data source,
            # but first drop primary df sectorconsumedby because empty
            flows_subset2 = flows_subset.drop(columns='SectorConsumedBy')
            fbs = flows_subset2.merge(fba_allocation_subset[['SectorProducedBy',
                                                             'SectorConsumedBy',
                                                             'FlowAmountRatio']],
                                      how='left')
        elif aset == 'facts_and_figures':
            # determine ratios of food waste by waste management pathway
            fba_allocation_wsec = fba_allocation_wsec.assign(
                Denominator=fba_allocation_wsec.groupby(
                    ['FlowName'])['FlowAmount'].transform('sum'))
            fba_allocation_wsec = fba_allocation_wsec.assign(
                FlowAmountRatio=fba_allocation_wsec['FlowAmount'] /
                                fba_allocation_wsec['Denominator'])

            # merge the primary data source and allocation data source,
            # but first drop primary df sectorconsumedby because empty
            flows_subset2 = flows_subset.drop(columns='SectorConsumedBy')
            # add temp merge col
            df_list = [flows_subset2, fba_allocation_wsec]
            for i, df in enumerate(df_list):
                df_list[i]['merge_col'] = 1
            fbs = flows_subset2.merge(fba_allocation_wsec[['SectorConsumedBy',
                                                           'FlowAmountRatio',
                                                           'merge_col']],
                                      how='left').drop(columns='merge_col')

        # calculate flow amounts for each sector
        log.info("Calculating new flow amounts using flow ratios")
        fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['FlowAmountRatio']

        # drop columns
        fbs = fbs.drop(columns=['FlowAmountRatio'])

        # check before and after totals
        # compare flowbysector with flowbyactivity
        compare_activity_to_sector_flowamounts(
            flows_subset, fbs, aset, method, v, attr)
        compare_fba_geo_subset_and_fbs_output_totals(
            flows_subset, fbs, aset, k, v, attr, method)

        activity_list.append(fbs)

    cnhw = pd.concat(activity_list)

    # for consistency with food waste m1 and with the data out of the EPA
    # WFR which is used for residential food waste, update the flowable from
    # 'Food' to 'Food Waste'
    cnhw['Flowable'] = 'Food Waste'

    return cnhw

