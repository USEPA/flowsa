# USGS_NMIC_SodaAsh.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
SourceName: USGS_NMIC_SodaAsh
https://www.usgs.gov/centers/nmic/soda-ash-statistics-and-information

Minerals Yearbook, xls file, tab "T4"
REPORTED CONSUMPTION OF SODA ASH IN THE UNITED STATES, BY END USE, BY QUARTER1

Interested in annual data, not quarterly
Years = 2010+
https://s3-us-west-2.amazonaws.com/prd-wret/assets/palladium/production/mineral-pubs/soda-ash/myb1-2010-sodaa.pdf
"""


def usgs_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    year = str(args["year"])
    url.replace("__year__", year)
    url.replace("__format__", config["formats"][year])
    return [url]


def usgs_call(url, usgs_response, args):
    """TODO."""
    return []


def usgs_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""

    for df in dataframe_list:
        # add columns at national and state level that only exist at the county level
        if 'state_cd' not in df:
            df['state_cd'] = '00'
        if 'state_name' not in df:
            df['state_name'] = ''
        if 'county_cd' not in df:
            df['county_cd'] = '000'
        if 'county_nm' not in df:
            df['county_nm'] = ''
        if 'year' not in df:
            df['year'] = args["year"]

    return df

