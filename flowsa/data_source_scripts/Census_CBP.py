# Census_CBP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls County Business Patterns data in NAICS from the Census Bureau
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import get_all_state_FIPS_2, get_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def Census_CBP_URL_helper(*, build_url, year, **_):
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
    # This section gets the census data by county instead of by state.
    # This is only for years 2010 and 2011. This is done because the State
    # query that gets all counties returns too many results and errors out.
    if year in ['2010', '2011']:
        county_fips_df = get_county_FIPS('2010')
        county_fips = county_fips_df.FIPS
        for d in county_fips:
            url = build_url
            state_digit = str(d[0]) + str(d[1])
            county_digit = str(d[2]) + str(d[3]) + str(d[4])
            url = url.replace("__NAICS__", "NAICS2007")
            url = url.replace("__stateFIPS__", state_digit)
            url = url.replace("__countyFIPS__", county_digit)

            if year == "2010":
                # These are the counties where data is not available.
                # s signifies state code and y indicates year.
                s_02_y_10 = ["105", "195", "198", "230", "275"]
                s_15_y_10 = ["005"]
                s_48_y_10 = ["269"]

                # There are specific counties in various states for the year
                # 2010 that do not have data. For these counties a URL is not
                # generated as if there is no data then an error occurs.
                if state_digit == "02" and county_digit in s_02_y_10 or \
                        state_digit == "15" and county_digit in s_15_y_10 or \
                        state_digit == "48" and county_digit in s_48_y_10:
                    pass
                else:
                    urls_census.append(url)
            else:
                # These are the counties where data is not available.
                # s signifies state code and y indicates year.
                s_02_y_11 = ["105", "195", "198", "230", "275"]
                s_15_y_11 = ["005"]
                s_48_y_11 = ["269", "301"]

                # There are specific counties in various states for the year
                # 2011 that do not have data. For these counties a URL is
                # not generated as if there is no data then an error occurs.
                if state_digit == "02" and county_digit in s_02_y_11 or \
                        state_digit == "15" and county_digit in s_15_y_11 or \
                        state_digit == "48" and county_digit in s_48_y_11:
                    pass
                else:
                    urls_census.append(url)
    else:
        FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
        for state in FIPS_2:
            url = build_url
            url = url.replace("__stateFIPS__", state)
            # specified NAICS code year depends on year of data
            if year in ['2012', '2013', '2014', '2015', '2016']:
                url = url.replace("__NAICS__", "NAICS2012")
            else:
                url = url.replace("__NAICS__", "NAICS2017")
            url = url.replace("__countyFIPS__", "*")
            urls_census.append(url)

    return urls_census


def census_cbp_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    cbp_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(
        data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
    return df_census


def census_cbp_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # Add year
    df['Year'] = year
    # convert county='999' to line for full state
    df.loc[df['county'] == '999', 'county'] = '000'
    # Make FIPS as a combo of state and county codes
    df['Location'] = df['state'] + df['county']

    naics_col = [c for c in df.columns if c.startswith('NAICS')][0]
    df = (df
          .drop(columns=['state', 'county'])
          .rename(columns={naics_col: 'ActivityProducedBy'})
          .assign(Description = naics_col)
          .query('ActivityProducedBy != "00"')
          )

    # use "melt" fxn to convert colummns into rows, also keep Flags and merge
    # them back in
    df1 = df.melt(
        id_vars=["Location", "ActivityProducedBy", "Year", "Description"],
        value_vars=['ESTAB', 'EMP', 'PAYANN'],
        var_name="FlowName",
        value_name="FlowAmount")
    df2 = df.melt(
        id_vars=["Location", "ActivityProducedBy", "Year", "Description"],
        value_vars=['ESTAB_F', 'EMP_F', 'PAYANN_F'],
        var_name="FlowName_F",
        value_name="Note")
    df2['FlowName'] = df2['FlowName_F'].str.replace('_F', '')
    df2 = df2.drop(columns=['FlowName_F'])
    df = pd.merge(df1, df2, on=["Location", "ActivityProducedBy", "Year",
                                "Description", "FlowName"])
    # Assign suppressed column
    df = (df.assign(
        Suppressed = np.where(df.Note.isnull(),
                              np.nan, df.Note),
        # FlowAmount = np.where(df.Note.isnull()),
        #                       0, df.FlowAmount)
        )
            .drop(columns='Note'))
    # specify unit based on flowname, payroll in units of thousdand USD
    df = (df
          .assign(Unit = lambda x: np.where(x['FlowName'] == 'PAYANN',
                                           'USD', 'p'))
          .assign(FlowAmount = lambda x: np.where(x['FlowName'] == 'PAYANN',
                                                  x['FlowAmount'].astype(float) * 1000,
                                                  x['FlowAmount']))
          .assign(FlowName = lambda x: x['FlowName'].map(
              {'ESTAB': 'Number of establishments',
               'EMP': 'Number of employees',
               'PAYANN': 'Annual payroll'}))
          )

    # specify class
    df['Class'] = np.select([df['FlowName'] == 'Number of employees',
                             df['FlowName'] == 'Number of establishments',
                             df['FlowName'] == 'Annual payroll'],
                            ['Employment', 'Other', 'Money'])
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_CBP'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_CBP', year='2016-2018')
    fba = flowsa.getFlowByActivity('Census_CBP', 2017)
