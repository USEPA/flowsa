# Census_ACS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
U.S. Census American Community Survey
5-yr Data Profile (DP03): Selected Economic Characteristics
5-yr Data Profile (DP04): Selected Housing Characteristics
5-yr Data Profile (DP05): Demographic and Housing Estimates
5-yr Subject Tables (S1401): School Enrollment
5-yr Subject Tables (S1501): Educational Attainment

variables: https://api.census.gov/data/2022/acs/acs5/profile/variables.html
"""
import json
import pandas as pd
from esupy.remote import make_url_request
from flowsa.flowbyfunctions import assign_fips_location_system


def load_acs_variables(param = 'profile'):
    data = make_url_request(f'https://api.census.gov/data/2022/acs/acs5/{param}/variables.json')
    variables = json.loads(data.content).get('variables')
    return variables

def DP_URL_helper(*, build_url, config, year, **_):
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
    urls = []

    for table, values in config['tables'].items():
        url = (build_url
               .replace('__param__', values['param'])
               .replace('__group__', values['group'])
               )
        urls.append(url)

    return urls


def DP_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    json_data = json.loads(resp.text)
    # convert response to dataframe
    df = pd.DataFrame(data=json_data[1:len(json_data)],
                      columns=json_data[0])
    return df

def parse_years(year):
    if '-' in str(year):
        years = str(year).split('-')
        year_iter = list(range(int(years[0]), int(years[1]) + 1))
        year_iter = [str(x) for x in year_iter]
    else:
        year_iter = [year]
    return year_iter


def DP_5yr_parse(*, df_list, config, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df_list2 = []
    for df0 in df_list:
        table = df0.columns[3].split('_')[0]
        # limit variable dictionary for select years
        table_dict = config['tables'][table]
        year_dict = {k: v for k,v in table_dict['variables'].items()
                     if year in parse_years(v.get('years', year))}
        description = {k: v['label'] for k,v in
                       load_acs_variables(table_dict['param']).items()
                       if k in year_dict.keys()}
        names = {k: v['name'] for k,v in year_dict.items()}
        units = {k: v['unit'] for k,v in year_dict.items()}

        # remove first string of GEO_ID to access the FIPS code
        df0['GEO_ID'] = df0.GEO_ID.str.replace('0500000US' , '')

        # melt economic columns into one FlowAmount column
        df0 = (df0.melt(id_vars= ['GEO_ID'], 
                      value_vars=year_dict.keys(),
                      var_name='code',
                      value_name='FlowAmount')
              .assign(FlowName = lambda x: x['code'].map(names))
              .assign(Unit = lambda x: x['code'].map(units))
              .assign(Description = lambda x: x['code'].map(description))
              .assign(Description = lambda x: 
                      x[['code', 'Description']].agg(': '.join, axis=1)
                      .str.replace('!!', ', '))
              .rename(columns={'GEO_ID':'Location'})
              )
        df_list2.append(df0)

    # TODO can the distribution data also be captured

    # concat dataframes
    df = pd.concat(df_list2, sort=False)

    # modify units
    df["Unit"] = (df['Unit']
                  .str.split(' ').str[0]
                  .replace("#", "p")
                  .replace("%", "Percent")
                  )

    # hard code data for flowsa format
    df = assign_fips_location_system(df, year)
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df.loc[df.Unit=='USD', 'Class'] = 'Money'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_ACS'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_ACS', year=2018)
    fba = flowsa.getFlowByActivity('Census_ACS', year=2018)
