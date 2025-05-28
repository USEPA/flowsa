# Census_Surveys.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
U.S. Census Annual Survey, including
Service Annual Survey (SAS)
Annual Retail Trade Survey (ARTS)
Annual Wholesale Trade Survey (AWTS)
"""
import pandas as pd
import numpy as np
import io
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS


def census_sas_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    for sheet, name in config['sheets'].items():
        df = (pd.read_excel(resp.content,
                            sheet_name=sheet,
                            header=4)
              .assign(sheet=f'{sheet}: {name}')
              )
        df_list.append(df)

    return df_list


def census_arts_url_helper(*, build_url, config, **_):
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
    urls = []
    for file in config.get('files').values():
        urls.append(build_url.replace('__file__', file))

    return urls


def census_awts_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    df = (pd.read_excel(io.BytesIO(resp.content), header=3)
          .drop(index=0)
          .reset_index(drop=True)
          )
    df.columns = df.columns.astype(str).str.strip()
    df_list.append(df)

    return df_list


def census_arts_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    df = (pd.read_excel(io.BytesIO(resp.content), header=3)
          )
    df_list.append(df)

    return df_list


def census_sas_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)
    value_vars = [c for c in df.columns if "Estimate" in c or
                  "Coefficient of Variation" in c]
    id_vars = [c for c in df.columns if c not in value_vars]
    df = (df.dropna(subset=['Item'])
            .melt(id_vars = id_vars,
                  value_vars = value_vars)
            .assign(Year = lambda x: x['variable'].str[0:4])
            .assign(var = lambda x: x['variable'].str[5:])
            .drop(columns='variable')
            .pivot_table(columns=['var'],
                                  index=id_vars + ['Year'],
                                  values='value', aggfunc='sum')
            .reset_index()
            .query('`Tax Status` == "All Establishments"')
            .rename(columns={'NAICS': 'ActivityConsumedBy',
                             'Item': 'FlowName',
                             'sheet': 'Description',
                             'Coefficient of Variation': 'Spread',
                             'Estimate': 'FlowAmount'})
            .drop(columns=['Employer Status', 'Tax Status', 'NAICS Description'])
            )


    # set suppressed values to 0 but mark as suppressed
    # otherwise set non-numeric to nan
    df = (df.assign(
            Suppressed = np.where(df.FlowAmount.str.strip().isin(["S", "Z", "D"]),
                                  df.FlowAmount.str.strip(),
                                  np.nan),
            FlowAmount = np.where(df.FlowAmount.str.strip().isin(["S", "Z", "D"]),
                                  0,
                                  df.FlowAmount)))
    df = (df.assign(
            Suppressed = np.where(df.FlowAmount.str.endswith('(s)') == True,
                                  '(s)',
                                  df.Suppressed),
            FlowAmount = np.where(df.FlowAmount.str.endswith('(s)') == True,
                                  df.FlowAmount.str.replace(',','').str[:-3],
                                  df.FlowAmount),
        ))

    df['Class'] = 'Money'
    df['SourceName'] = 'Census_SAS'
    # millions of dollars
    df['FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    df['Spread'] = pd.to_numeric(df['Spread'], errors='coerce')
    df['MeasureofSpread'] = 'Coefficient of Variation'
    df['Unit'] = 'USD'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Compartment'] = None
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df


def census_awts_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)

    # Drop footnotes
    row = df[df.iloc[:, 0].astype(str).str.contains('Notes').fillna(False)].index[0]
    df = df.iloc[:row]

    cols = list(df.columns[0:2])
    df = (df
          .drop(columns=['Kind of Business', 'Type of Operation'])
          # .fillna("NA")
          .melt(id_vars=cols, var_name='Year', value_name='Amount')
          .assign(Year = lambda x: x['Year'].str[0:4])
          .rename(columns = {cols[0]: 'ActivityConsumedBy',
                             cols[1]: 'FlowName'})
          )
    df['Amount'] = df.Amount.fillna("NA")

    # Updated suppressed data field
    df = (df.assign(
        Suppressed = np.where(df.Amount.isin(["S", "NA", "x"]),
                              df.Amount, np.nan),
        FlowAmount = np.where(df.Amount.isin(["S", "NA", "x"]),
                              0, df.Amount))
        )

    df['FlowAmount'] = df['FlowAmount'].astype(float)
    df = (df.assign(
        FlowAmount = np.where(df.FlowName.str.contains('percent'),
                              df.FlowAmount, df.FlowAmount * 1000000),
        Unit = np.where(df.FlowName.str.contains('percent'),
                        'Percent', 'USD'))
        )

    df = (df
          .drop(columns=['Amount'])
          .assign(Class = "Money")
          .assign(SourceName = 'Census_AWTS')
          .assign(Compartment = None)
          .assign(FlowType = "TECHNOSPHERE_FLOW")
          .assign(Location = US_FIPS)
          # .assign(Description = '')
          .pipe(assign_fips_location_system, 2024)
          )

    # Temp data quality
    df = (df
          .assign(DataReliability = 5)
          .assign(DataCollection = 5)
          )

    return df


def census_arts_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df_list1 = []
    for df in df_list:
        # Drop footnotes
        df = df.dropna(subset='Kind of Business')
        df.loc[0, 'NAICS Code'] = "Total"
        name = df.loc[0, 'Kind of Business'].split(',')[0]
        df = df.assign(FlowName = name)
        df_list1.append(df)
    df = pd.concat(df_list1, sort=False)

    df = (df
          .dropna(subset='NAICS Code')
          .drop(columns=['Kind of Business'])
          .melt(id_vars=['NAICS Code', 'FlowName'], var_name='Year', value_name='Amount')
          .rename(columns = {'NAICS Code': 'ActivityConsumedBy'})
          .assign(Year = lambda x: x['Year'].astype(str))
          )

    # Updated suppressed data field
    df = (df.assign(
        Suppressed = np.where(df.Amount.isin(["S", "NA", "x"]),
                              df.Amount, np.nan),
        FlowAmount = np.where(df.Amount.isin(["S", "NA", "x"]),
                              0, df.Amount))
        )

    df['FlowAmount'] = np.where(df['FlowName'].str.contains('percentage'),
                                df['FlowAmount'].astype(float),
                                df['FlowAmount'].astype(float) * 1000000)

    df = (df
          .drop(columns=['Amount'])
          .assign(Class = "Money")
          .assign(Unit = np.where(df['FlowName'].str.contains('percentage'),
                                  'percent', 'USD'))
          .assign(SourceName = 'Census_ARTS')
          .assign(Compartment = None)
          # .assign(ActivityProducedBy = '')
          .assign(FlowType = "TECHNOSPHERE_FLOW")
          # .assign(Description = '')
          .assign(Location = US_FIPS)
          .pipe(assign_fips_location_system, 2024)
          )

    # Temp data quality
    df = (df
          .assign(DataReliability = 5)
          .assign(DataCollection = 5)
          )

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='Census_ARTS', year='2013-2022')
    flowsa.generateflowbyactivity.main(source='Census_AWTS', year='2013-2022')
    fba = flowsa.getFlowByActivity('Census_ARTS', 2022)
