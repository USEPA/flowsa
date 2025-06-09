# Census_EC.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls U.S. Census Bureau Economic Census Data
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowbyactivity import FlowByActivity


def census_EC_URL_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    for k, v in config['datasets'].items():
        for dataset in v.get(year, []):
            url = (build_url
                   .replace('__dataset__', k)
                   .replace('__group__', f'group({dataset})')
                   )
            if year == '2012':
                # for 2012 need both us and state call separately
                url += '&for=us:*'
                urls_census.append(url)
                url = url.replace('&for=us:*', '&for=state:*')
                urls_census.append(url)
            else:
                urls_census.append(url)

    return urls_census


def census_EC_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    census_json = json.loads(resp.text)
    url = resp.url
    desc = url[url.find("(")+1:url.find(")")] # extract the group from the url
    # convert response to dataframe
    df = pd.DataFrame(data=census_json[1:len(census_json)],
                      columns=census_json[0])
    df = df.assign(Description = desc)
    return df


def census_EC_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    if(year == '2017'):
        df = (df
              .query('TAXSTAT_LABEL == "All establishments"')
              .query('TYPOP_LABEL == "All establishments"')
              )
        class_label = 'CLASSCUST_LABEL'
    else:
        class_label = 'CLASSCUST_TTL'

    df = (df
          .filter([f'NAICS{year}', class_label, 'ESTAB', 'RCPTOT', 'RCPTOT_F',
                   'GEO_ID', 'RCPTOT_DIST', 'YEAR', 'Description'])
          .rename(columns={f'NAICS{year}': 'ActivityProducedBy',
                           f'{class_label}': 'ActivityConsumedBy',
                           'ESTAB': 'Number of establishments',
                           'RCPTOT': 'Sales, value of shipments, or revenue',
                           'RCPTOT_DIST': 'Distribution of sales, value of shipments, or revenue',
                           'RCPTOT_F': 'Note',
                           'YEAR': 'Year'})
          .assign(Location = lambda x: x['GEO_ID'].str[-2:])
          .melt(id_vars=['ActivityProducedBy', 'ActivityConsumedBy',
                         'Location', 'Year', 'Description', 'Note',],
                value_vars=['Number of establishments',
                            'Sales, value of shipments, or revenue',
                            'Distribution of sales, value of shipments, or revenue'],
                value_name='FlowAmount',
                var_name='FlowName')
          .assign(FlowAmount = lambda x: x['FlowAmount'].astype(float))
          )

    # Updated suppressed data field
    df = (df.assign(
        Suppressed = np.where(df.Note.isin(["D"]),
                              df.Note, np.nan),
        FlowAmount = np.where(df.Note.isin(["D"]),
                              0, df.FlowAmount))
            .drop(columns='Note'))

    conditions = [df['FlowName'] == 'Number of establishments',
                  df['FlowName'] == 'Sales, value of shipments, or revenue',
                  df['FlowName'] == 'Distribution of sales, value of shipments, or revenue']
    df['Unit'] = np.select(conditions, ['p', 'USD', 'Percent'])
    df['Class'] = np.select(conditions, ['Other', 'Money', 'Money'])
    df['FlowAmount'] = np.where(df['FlowName'] == 'Sales, value of shipments, or revenue',
                                df['FlowAmount'] * 1000,
                                df['FlowAmount'])
    df['Location'] = np.where(df['Location'] == 'US', US_FIPS,
                              df['Location'].str.pad(5, side='right', fillchar='0'))

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_EC'
    df['FlowType'] = "ELEMENTARY_FLOW"
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def census_EC_PxI_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    prd_by_ind = df['Description'] == 'EC1700NAPCSPRDIND' # Boolean mask
    df = (df
          .filter([f'NAICS{year}', 'INDGROUP', f'NAPCS{year}',
                   f'NAPCS{year}_LABEL',
                   'NAPCSDOL', 'NAPCSDOL_F', 'NAPCSDOL_S',
                   'GEO_ID', 'YEAR'])
          .rename(columns={f'NAICS{year}': 'Industry',
                           f'NAPCS{year}': 'Product',
                           f'NAPCS{year}_LABEL': 'Description',
                           'NAPCSDOL': 'Sales, value of shipments, or revenue',
                           'NAPCSDOL_F': 'Flag',
                           'NAPCSDOL_S': 'Relative standard error',
                           'YEAR': 'Year'})
          .assign(Location = lambda x: x['GEO_ID'].str[-2:])
          )

    df = (df
          .assign(FlowName = df['Product'])
          .assign(ActivityConsumedBy = '')
          .assign(ActivityProducedBy = df['Industry'])
          ## TODO confirm assignment of FlowName, ACB, APB
          # .assign(FlowName = np.where(prd_by_ind,
          #     df['Product'],
          #     df['Industry']))
          # .assign(ActivityConsumedBy = np.where(prd_by_ind,
          #     df['Industry'], ''))
          # .assign(ActivityProducedBy = np.where(prd_by_ind,
          #     '', df['Product']))
          # .assign(Description = np.where(prd_by_ind,
          #     'Product by Industry',
          #     'Industry by Product'))
          .rename(columns={'Relative standard error': 'Spread',
                           'Sales, value of shipments, or revenue': 'FlowAmount'})
          .assign(MeasureofSpread = 'Relative standard error')
          .assign(FlowAmount = lambda x: x['FlowAmount'].astype(float))
          )

    # Updated suppressed data field
    df = (df.assign(
        Suppressed = np.where(df.Flag.isin(["D", "s", "A", "S"]),
                              df.Flag, np.nan),
        FlowAmount = np.where(df.Flag.isin(["D", "s", "A", "S"]),
                              0, df.FlowAmount * 1000))
            .drop(columns='Flag'))

    df['Location'] = np.where(df['Location'] == 'US', US_FIPS,
                              df['Location'].str.pad(5, side='right', fillchar='0'))

    df = assign_fips_location_system(df, year)
    df['Unit'] = 'USD'
    df['Class'] = 'Money'
    df['SourceName'] = 'Census_EC_PxI'
    df['FlowType'] = "ELEMENTARY_FLOW"
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def move_flow_to_ACB(fba: FlowByActivity, **_) -> FlowByActivity:
    """clean_fba_before_activity_sets fxn
    """
    ## this moves the flow name (NAPCS code) to ACB and temporarily stores the APB
    # as the flow name, so that it doesn't get dropped when we try to map
    # NAPCS to NAICS
    fba = (fba
            # .query('ActivityProducedBy != "00"') # done in method yaml
            .assign(ActivityConsumedBy = fba['FlowName'])
            .assign(FlowName = fba['ActivityProducedBy'])
            .assign(ActivityProducedBy = None)
            )
    return fba

def keep_wholesale_retail(fba: FlowByActivity, **_) -> FlowByActivity:
    """clean_fba
    """
    # Keep only those NAPCS codes that start with 4 (Wholesale) or 5 (Retail)
    keep_sectors = ("4", "5")
    fba = (fba
           .query('ActivityConsumedBy.str.startswith(@keep_sectors)')
           )

    return fba


def clean_after_attr_m1(fba: FlowByActivity, **_) -> FlowByActivity:
    """clean_fba_after_attribution
    """
    # After mapping Census product code to NAICS, moves the FlowName (original Census NAICS) back
    # to SectorProducedBy with out mapping to leave the original data in place
    # Also resets the Flowable as the original NAPCS code for posterity

    fba = (fba
           .assign(SectorProducedBy = fba['Flowable'])
           .assign(Flowable = fba['ActivityConsumedBy'])
           )

    return fba

def clean_after_attr_m2(fba: FlowByActivity, **_) -> FlowByActivity:
    """clean_fba_after_attribution
    """
    # After mapping Census product code to NAICS, moves the FlowName (original Census NAICS) back
    # to SectorProducedBy with out mapping to leave the original data in place
    # Also moves the SCB back to the Flowable before resetting SCB
    # Flags context as Wholesale or Retail based on Census product code

    fba = (fba
           .assign(SectorProducedBy = fba['Flowable'])
           .assign(Flowable = fba['SectorConsumedBy'])
           .assign(SectorConsumedBy = np.nan)
           .assign(Context = lambda x: np.where(
               x['ActivityConsumedBy'].str.startswith('4'),
               'Wholesale','Retail'))
           )

    return fba



if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_EC_PxI', year=2017)
    fba = flowsa.getFlowByActivity('Census_EC_PxI', 2017)
