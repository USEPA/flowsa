# write_FIPS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Grabs FIPS codes from static URLs and creates crosswalk over the years.

- Shapes the set to include State and County names for all records.
- Writes reshaped file to datapath as csv.
"""

import io
import pandas as pd
from esupy.remote import make_url_request
from flowsa.common import clean_str_and_capitalize
from flowsa.settings import datapath


def stripcounty(s):
    """
    Removes " County" from county name
    :param s: a string ending with " County"
    :return:
    """
    if s.__class__ == str:
        if s.endswith(" County"):
            s = s[0:len(s)-7]
    return s


def annual_fips(years):
    """
    Fxn to pull the FIPS codes/names from the Census website. Columns are
    renamed amd subset.
    :param years: list, years to include in FIPS crosswalk
    :return:
    """

    df_list = {}
    for year in years:
        # only works for 2015 +....contacted Census on 5/1 to ask for county
        # level fips for previous years
        if year == '2013':
            url = 'https://www2.census.gov/programs-surveys/popest/geographies/' + \
                  year + '/all-geocodes-v' + year + '.xls'
        else:
            url = "https://www2.census.gov/programs-surveys/popest/geographies/" + \
                  year + "/all-geocodes-v" + year + ".xlsx"

        r = make_url_request(url)
        raw_df = pd.read_excel(io.BytesIO(r.content)).dropna().reset_index(
            drop=True)

        # skip the first few rows
        FIPS_df = pd.DataFrame(raw_df.loc[1:]).reindex()
        # Assign the column titles (remove whitespace if exists and new lines
        FIPS_df.columns = (raw_df.loc[0, ]
                           .str.replace('\n', '')
                           .str.replace(' ', '')
                           )

        original_cols = FIPS_df.columns

        # Create a dictionary of geographic levels
        geocode_levels = {"010": "Country",
                          "040": "State",
                          "050": "County_" + year}
        level_codes = list(geocode_levels.keys())
        # filter df for records with the levels of interest
        FIPS_df = FIPS_df.query(f"SummaryLevel.isin({level_codes})")

        # split df by level to return a list of dfs
        # use a list comprehension to split it out
        FIPS_bylevel = [pd.DataFrame(y) for x, y in FIPS_df.groupby(
            "SummaryLevel", as_index=False)]

        # Assume df order in list is in geolevels keys order

        # country does not have its own field
        state_and_county_fields = {
            "Country": ["StateCode(FIPS)"],
            "State": ["StateCode(FIPS)"],
            "County_" + year: ["StateCode(FIPS)", "CountyCode(FIPS)"]}

        name_field = "AreaName(includinglegal/statisticalareadescription)"

        new_dfs = {}
        for df in FIPS_bylevel:
            df = df.reset_index(drop=True)
            level = geocode_levels[df.loc[0, "SummaryLevel"]]
            new_df = df[original_cols]
            new_df = new_df.rename(columns={name_field: level})
            fields_to_keep = [str(x) for x in state_and_county_fields[level]]
            fields_to_keep.append(level)
            new_df = new_df[fields_to_keep]
            # Write each to the list
            new_dfs[level] = new_df

        # New merge the new dfs to add the info
        for k, v in new_dfs.items():
            fields_to_merge = [str(x) for x in state_and_county_fields[k]]
            FIPS_df = pd.merge(FIPS_df, v, on=fields_to_merge, how="left")

        # combine state and county codes
        FIPS_df['FIPS_' + year] = \
            FIPS_df[state_and_county_fields["County_" + year][0]].astype(
                str) + FIPS_df[state_and_county_fields["County_" + year][
                1]].astype(str)

        fields_to_keep = ["State", "County_" + year, "FIPS_" + year]
        FIPS_df = FIPS_df[fields_to_keep]

        # Clean the county field - remove the " County"
        FIPS_df["County_" + year] = FIPS_df["County_" + year].apply(
            stripcounty)
        FIPS_df["County_" + year] = FIPS_df["County_" + year].apply(
            clean_str_and_capitalize)
        FIPS_df["State"] = FIPS_df["State"].apply(clean_str_and_capitalize)

        # add to data dictionary of fips years
        df_list["FIPS_" + year] = FIPS_df
    return df_list


def annual_fips_name(df_fips_codes, years):
    """Add county names for years (if county names exist)"""
    df = df_fips_codes
    for year in years:
        df = pd.merge(df, fips_dic['FIPS_' + year], on='FIPS_' + year)
    return df


def read_fips_2010():
    """
    Read the 2010 FIPS from census website
    :return: df with FIPS 2010 codes
    """
    # read in 2010 fips county names
    names_10 = pd.read_excel(
        'https://www2.census.gov/programs-surveys/demo/reference-files'
        '/eeo/time-series/eeo-county-sets-2010.xls')
    # Assign the column titles
    names_10.columns = names_10.loc[2, ]
    # skip the first few rows
    names_10 = pd.DataFrame(names_10.loc[4:]).reset_index(drop=True)
    # drop rows of na
    names_10 = names_10.loc[~names_10['2010 County Set Description'].isna()]
    names_10 = names_10.loc[~names_10['FIPS County Code'].isna()].reset_index(
        drop=True)
    # new column of fips
    names_10['FIPS_2010'] = names_10['FIPS State Code'].astype(str) +\
                            names_10['FIPS County Code'].astype(str)
    # rename columns and subset df
    names_10 = names_10.rename(columns={'2010 County Set Description':
                                            'County_2010'})
    names_10 = names_10[['FIPS_2010', 'County_2010']]
    # drop empty fips column
    names_10['FIPS_2010'] = names_10['FIPS_2010'].str.strip()
    names_10 = names_10.loc[names_10['FIPS_2010'] != '']
    names_10 = names_10.sort_values(['FIPS_2010']).reset_index(drop=True)
    return names_10


if __name__ == '__main__':

    # consider modifying to include data for all years, as there are county
    # level name changes

    # years data interested in (list)
    years = ['2015']

    # read in the fips data dictionary
    fips_dic = annual_fips(years)

    # map county changes, based on FIPS 2015 df, using info from Census website
    # https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html
    # Accessed 04/10/2020
    df = fips_dic['FIPS_2015']

    # modify columns depicting how counties have changed over the years -
    # starting 2010

    # 2013 had two different/renamed fips
    df_13 = pd.DataFrame(df['FIPS_2015'])
    df_13['FIPS_2013'] = df_13['FIPS_2015']
    df_13.loc[df_13['FIPS_2013'] == "02158", 'FIPS_2013'] = "02270"
    df_13.loc[df_13['FIPS_2013'] == "46102", 'FIPS_2013'] = "46113"

    # # 2013 had a fips code that was merged with an existing fips,
    # so 2010 will have an additional row
    df_10 = pd.DataFrame(df_13["FIPS_2013"])
    df_10['FIPS_2010'] = df_13['FIPS_2013']
    df_10 = pd.concat([df_10, pd.DataFrame([["51019", "51515"]],
                                      columns=df_10.columns)])

    # merge 2010 with 2013 dataframe
    df2 = pd.merge(df_10, df_13, on="FIPS_2013", how='left')\

    # fips years notes
    # 2010, 2011, 2012       have same fips codes
    # 2013, 2014             have same fips codes
    # 2015, 2016, 2017, 2018 have same fips codes
    # 2019                   have same fips codes

    # Use Census data to assign county names to FIPS years.
    # Some county names have changed over the years,
    # while FIPS remain unchanged
    df3 = annual_fips_name(df2, years)

    # read in fips county names for each year
    names_10 = read_fips_2010()
    annual_names = annual_fips(['2013', '2019'])
    names_13 = annual_names['FIPS_2013']

    df4 = pd.merge(df3, names_10, on="FIPS_2010", how='left')
    df4 = pd.merge(df4, names_13, on=["State", "FIPS_2013"], how='left')

    # reorder dataframe
    fips_xwalk = df4[['State', 'FIPS_2010', 'County_2010', 'FIPS_2013',
                      'County_2013', 'FIPS_2015', 'County_2015']]
    fips_xwalk = fips_xwalk.sort_values(['FIPS_2010', 'FIPS_2013',
                                         'FIPS_2015'])
    # drop peurto rico data
    fips_xwalk = fips_xwalk[fips_xwalk['State'] != 'Puerto rico'].reset_index(
        drop=True)

    # write fips crosswalk as csv
    fips_xwalk.to_csv(f"{datapath}/FIPS_Crosswalk.csv", index=False)
