# EPAN_NI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Projects
/
FLOWSA
/
Years = 2012
Eliminated all columns with fraction in the title.
"""
import io
import pandas as pd
import xml.etree.ElementTree as ET
from esupy.remote import make_url_request


def url_file(url):
    url_array = url.split("get/")
    url_file = url_array[1]
    return url_file


def column_names(file_name):
    base_url = 'https://www.sciencebase.gov/catalog/file/get/'
    pacific_region = ['5d407318e4b01d82ce8d9b3c?f=__disk__22%2F5c%2Fe3%2F225'
                      'ce31141477eb0904f38f95f1d472bbe2a2a11',
                      '5d407318e4b01d82ce8d9b3c?f=__disk__2b%2F75%2F2b%2F2b7'
                      '52b0c5decf8e83c035d559a2688c481bb0cfe']
    midwestern = ['5cbf5150e4b09b8c0b700df3?f=__disk__66%2F4f%2Ff2%2F664ff289'
                  '064560bbce748082f7b34593dad49ca2',
                  '5cbf5150e4b09b8c0b700df3?f=__disk__bf%2F73%2F1f%2Fbf731fdf'
                  '4e984a5cf50c0f1a140cda366cb8c1d3']
    northeastern = ['5d4192aee4b01d82ce8da477?f=__disk__c2%2F02%2F06%2Fc202060'
                    '78520c5ec87394a3499eea073f472a27d',
                    '5d4192aee4b01d82ce8da477?f=__disk__b0%2Fb9%2F35%2Fb0b9350'
                    '21a47ccf57f7584cc7f14d82aacc491d1']
    southwestern = ['5f8f1f1282ce06b040efc90e?f=__disk__f8%2Fb8%2Ff9%2Ff8b8f9'
                    'bdc2a07f014ed6dced8feb2dd7bc63e056',
                    '5f8f1f1282ce06b040efc90e?f=__disk__8e%2F8e%2Fb8%2F8e8eb8'
                    '203ea14ab19a45372919a0dbf667d033b2']
    southeastern = ['5d6e70e5e4b0c4f70cf635a1?f=__disk__fb%2Fdb%2F92%2Ffbdb928'
                    '1872069b23bcd134a4c5fa1ddc7280b53',
                    '5d6e70e5e4b0c4f70cf635a1?f=__disk__14%2Fc1%2F63%2F14c1636'
                    'eef91529f548d5fe29ff3f426d3b4b996']
    if file_name in pacific_region:
        legend_name = "5d407318e4b01d82ce8d9b3c?f=__disk__ab%2F27%2F08%2Fab" \
                      "27083f354bd851ec09bc0f33c2dc130f808bb5"
    elif file_name in midwestern:
        legend_name = "5cbf5150e4b09b8c0b700df3?f=__disk__a6%2Ffb%2Fd6%2Fa6f" \
                      "bd6f6bcce874109d2e989d1d4d5a67c33cd49"
    elif file_name in northeastern:
        legend_name = "5d4192aee4b01d82ce8da477?f=__disk__81%2F5d%2F3d%2F815" \
                      "d3deb08f82c1662ff94eb941074ff99c75088"
    elif file_name in southwestern:
        legend_name = "5f8f1f1282ce06b040efc90e?f=__disk__44%2Ff6%2F74%2F44f" \
                      "674b54b2fa571191a597c8dfae0923893d3d3"
    elif file_name in southeastern:
        legend_name = "5d6e70e5e4b0c4f70cf635a1?f=__disk__93%2Fba%2F5c%2F93b" \
                      "a5c50c58ced4116ad2e5b9783fc7848ab2cb5"
    contents = make_url_request(base_url + legend_name)
    xslt_content = contents.content.decode('utf-8')
    root = ET.fromstring(xslt_content)
    label = []
    name = []
    for attr in root.iter('attr'):
        for child in attr:
            if str(child.tag) == 'attrlabl':
                label.append(str(child.text))
            if str(child.tag) == 'attrdef':
                name.append(str(child.text))
    legend = pd.DataFrame()
    legend["label"] = label
    legend["name"] = name
    return legend


def name_and_unit_split(df_legend):
    for i in range(len(df_legend)):
        apb = df_legend.loc[i, "name"]
        apb_str = str(apb)
        if ',' in apb_str:
            apb_split = apb_str.split(',')
            activity = apb_split[0]
            unit_str = apb_split[1]
            unit_list = unit_str.split('/')
            unit = unit_list[0]
            df_legend.loc[i, "name"] = activity
            df_legend.loc[i, "Unit"] = unit
        else:
            df_legend.loc[i, "Unit"] = None
    return df_legend


def name_replace(df_legend, df_raw):
    for col_name in df_raw.columns:
        for i in range(len(df_legend)):
            if col_name == df_legend.loc[i, "label"]:
                if col_name.lower() != "comid":
                    df_raw = df_raw.rename(
                        columns={col_name: df_legend.loc[i, "name"]})
    return df_raw


def sparrow_url_helper(*, build_url, config, **_):
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
    # initiate url list for coa cropland data
    urls = []
    # replace "__xlsx_name__" in build_url to create three urls
    for x in config['txt']:
        url = build_url
        url = url.replace("__txt__", x)
        urls.append(url)
    return urls


def sparrow_call(*, resp, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param url: string, url
    :return: pandas dataframe of original source data
    """
    text = resp.content

    ph = ['5cbf5150e4b09b8c0b700df3?f=__disk__bf%2F73%2F1f%2Fbf731fdf4e984'
          'a5cf50c0f1a140cda366cb8c1d3',
          '5d407318e4b01d82ce8d9b3c?f=__disk__2b%2F75%2F2b%2F2b752b0c5decf8e'
          '83c035d559a2688c481bb0cfe',
          '5d4192aee4b01d82ce8da477?f=__disk__b0%2Fb9%2F35%2Fb0b935021a47ccf5'
          '7f7584cc7f14d82aacc491d1',
          '5f8f1f1282ce06b040efc90e?f=__disk__8e%2F8e%2Fb8%2F8e8eb8203ea14ab1'
          '9a45372919a0dbf667d033b2',
          '5d6e70e5e4b0c4f70cf635a1?f=__disk__14%2Fc1%2F63%2F14c1636eef91529f'
          '548d5fe29ff3f426d3b4b996']
    ni = ['5cbf5150e4b09b8c0b700df3?f=__disk__66%2F4f%2Ff2%2F664ff289064560bb'
          'ce748082f7b34593dad49ca2',
          '5d407318e4b01d82ce8d9b3c?f=__disk__22%2F5c%2Fe3%2F225ce31141477eb09'
          '04f38f95f1d472bbe2a2a11',
          '5d4192aee4b01d82ce8da477?f=__disk__c2%2F02%2F06%2Fc20206078520c5ec'
          '87394a3499eea073f472a27d',
          '5f8f1f1282ce06b040efc90e?f=__disk__f8%2Fb8%2Ff9%2Ff8b8f9bdc2a07f01'
          '4ed6dced8feb2dd7bc63e056',
          '5d6e70e5e4b0c4f70cf635a1?f=__disk__fb%2Fdb%2F92%2Ffbdb9281872069b2'
          '3bcd134a4c5fa1ddc7280b53']
    comid_cap = ["5f8f1f1282ce06b040efc90e?f=__disk__8e%2F8e%2Fb8%2F8e8eb8203e"
                 "a14ab19a45372919a0dbf667d033b2",
                 "5f8f1f1282ce06b040efc90e?f=__disk__f8%2Fb8%2Ff9%2Ff8b8f9bdc"
                 "2a07f014ed6dced8feb2dd7bc63e056"]
    url_file_name = url_file(url)
    legend = column_names(url_file_name)
    legend = name_and_unit_split(legend)

    if url_file_name in ph:
        chem_type = "Phosphorus"
    else:
        chem_type = "Nitrogen"

    for i in range(len(legend)):
        if "incremental" in legend.loc[i, "name"].lower():
            legend.loc[i, "FlowName"] = chem_type + ' incremental'
        elif "accumulated" in legend.loc[i, "name"].lower():
            legend.loc[i, "FlowName"] = chem_type + ' accumulated'
        elif "cumulated" in legend.loc[i, "name"].lower():
            legend.loc[i, "FlowName"] = chem_type + ' cumulated'
        elif "mean" in legend.loc[i, "name"].lower():
            legend.loc[i, "FlowName"] = chem_type + ' mean'
        elif "Total upstream watershed area" in legend.loc[i, "name"].lower():
            legend.loc[i, "FlowName"] = chem_type + ' total'
        else:
            legend.loc[i, "FlowName"] = chem_type

    if "5d407318e4b01d82ce8d9b3c?f=__disk__2b%2F75%2F2b%2F2b752b0c5decf8e83" \
       "c035d559a2688c481bb0cfe" in url:
        df_raw = pd.read_csv(io.StringIO(text.decode('utf-8')), sep='\t')

    else:
        df_raw = pd.read_csv(io.StringIO(text.decode('utf-8')))
    df_raw = name_replace(legend, df_raw)
    legend = legend.drop(columns=['label'])
    legend = legend.rename(columns={"name": "ActivityProducedBy"})

    if url_file_name in comid_cap:
        df_raw = df_raw.rename(columns={"COMID": "comid"})

    df_spread = pd.DataFrame()
    df_no_spread = pd.DataFrame()
    for column_name in df_raw.columns:
        if "fraction" in column_name.lower() or "flux" in column_name.lower():
            df_raw = df_raw.drop(columns=[column_name])
        elif "standard error" in column_name.lower():
            df_spread[column_name] = df_raw[column_name]
            df_raw = df_raw.drop(columns=[column_name])
    spread_coul = []
    for cn in df_spread.columns:
        if "Standard error for " in cn:
            c_name = cn.split("Standard error for ")
            df_spread = df_spread.rename(columns={cn: c_name[1].capitalize()})
            spread_coul.append(c_name[1].capitalize())
        else:
            c_name = cn.split("standard error")
            spread_coul.append(c_name[0].capitalize())

    for column_name in df_raw.columns:
        if column_name not in spread_coul and column_name != "comid":
            df_no_spread[column_name] = df_raw[column_name]
            df_raw = df_raw.drop(columns=[column_name])

    df_no_spread["comid"] = df_raw["comid"]
    df_spread["comid"] = df_raw["comid"]

    # use "melt" fxn to convert colummns into rows
    df = df_raw.melt(id_vars=["comid"],
                     var_name="ActivityProducedBy",
                     value_name="FlowAmount")
    df = df.rename(columns={"comid": "Location"})

    df_spread = df_spread.melt(id_vars=["comid"],
                               var_name="spread_name",
                               value_name="Spread")
    df_spread = df_spread.rename(columns={"comid": "Location"})
    df_spread = df_spread.rename(columns={"spread_name": "ActivityProducedBy"})
    df_spread["MeasureofSpread"] = 'SE'

    df_no_spread = df_no_spread.melt(id_vars=["comid"],
                                     var_name="ActivityProducedBy",
                                     value_name="FlowAmount")
    df_no_spread = df_no_spread.rename(columns={"comid": "Location"})
    df_no_spread = pd.merge(df_no_spread, legend, on="ActivityProducedBy")
    df = pd.merge(df, legend, on="ActivityProducedBy")
    df = pd.merge(df, df_spread, left_on=["ActivityProducedBy", "Location"],
                  right_on=["ActivityProducedBy", "Location"])
    dataframes = [df, df_no_spread]
    df1 = pd.concat(dataframes)
    df1.reset_index(drop=True)
    return df1


def sparrow_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.DataFrame()
    dataframe = pd.DataFrame()
    for df in df_list:
        df["Compartment "] = "ground"
        df["Class"] = "Chemicals"
        df["SourceName"] = "USGS_SPARROW"
        df["LocationSystem"] = 'HUC'
        df["Year"] = str(year)
        df["ActivityConsumedBy"] = None

    dataframe = pd.concat(df_list, ignore_index=True)

    return dataframe
