# EPA_StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US GHGs from EPA disaggregated to States
"""
import pandas as pd
import io
from zipfile import ZipFile

import flowsa.flowbyactivity
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbysector import FlowBySector
from flowsa.flowsa_log import log
from flowsa.location import apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
import flowsa.exceptions


def epa_state_ghgi_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe
    :param resp: response from url call
    :param config: dictionary, items in FBA method yaml
    :return: pandas dataframe of original source data
    """
    with ZipFile(io.BytesIO(resp.content)) as z:
        df = pd.read_excel(z.open(config['file']),
                           sheet_name=config['sheet'])
    return df

def epa_state_ghgi_parse(*, df_list, source, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data_df = pd.concat(df_list)

    activity_cols = ['econ_sector', 'econ_subsector', 'subsector',
                     'category', 'fuel1', 'fuel2', 'sub_category_1',
                     'sub_category_2', 'sub_category_3', 'sub_category_4', 'sub_category_5']

    states = data_df[['geo_ref']].drop_duplicates()
    flows = data_df[['ghg']].drop_duplicates()
    year_cols = [col for col in data_df if col.startswith('Y') and len(col)==5]
    df = (data_df.melt(id_vars = activity_cols + ['geo_ref'] + ['ghg'],
                       value_vars=year_cols,
                       var_name = 'Year',
                       value_name = 'FlowAmount')
                .assign(Year = lambda x: x['Year'].str[1:])
                .assign(Unit = 'MMT CO2e') # Updated from Tg
                .assign(FlowType = 'ELEMENTARY_FLOW')
                .assign(SourceName = source)
                .assign(Class = 'Chemicals')
                .assign(Compartment = 'air')
                .rename(columns={'geo_ref': 'State',
                                 'ghg': 'FlowName'})
                .assign(ActivityProducedBy = lambda x: x[activity_cols]
                        .apply(lambda row: " - ".join(
                            row.dropna().drop_duplicates().astype(str)),
                               axis=1))
                .drop(columns=activity_cols)
                )

    activities = df[['ActivityProducedBy']].drop_duplicates()

    df = apply_county_FIPS(df)
    df = assign_fips_location_system(df, '2015')
    df.drop(columns=['County'], inplace=True)

    return df


def tag_biogenic_activities(fba, source_dict, **_):
    """
    clean_fba_before_mapping_df_fxn to tag emissions from passed activities
    as biogenic. Activities passed as list in paramter 'activity_list'.
    """
    a_list = source_dict.get('activity_list')
    if a_list is None:
        raise flowsa.exceptions.FBSMethodConstructionError(
            message="Activities to tag must be passed in FBS parameter "
            "'activity_list'")
    fba.loc[fba['ActivityProducedBy'].isin(a_list),
            'FlowName'] = fba['FlowName'] + ' - biogenic'

    return fba


def allocate_flows_by_fuel(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    clean_fba_before_activity_sets fxn to estimate CH4 and N2O emissions by
    fuel type, using ratios derived from the national inventory as proxy

    returns a FBA that has increased in length x-times based on the number of
    fuels; Fuel is added to "Description" field; total FlowAmount remains
    unchanged.
    """
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    year = fba.config.get('year')
    # combine lists of activities from CO2 activity set
    alist = fba.config['clean_parameter']['flow_ratio_source']
    if any(isinstance(i, list) for i in alist):
        # pulled from !index, so list of lists
        activity_list = sum(alist, [])
    else:
        activity_list = alist
    source_fba = pd.concat([
        flowsa.flowbyactivity.getFlowByActivity(x, year) for x in
        fba.config['clean_parameter']['fba_source']
        ], ignore_index=True)

    sector = fba.config['clean_parameter']['sector']

    # align fuel names from National GHGI (keys) with StateGHGI (values)
    fuels = {'Natural Gas': 'Natural Gas',
             'Coal': 'Coal',
             'Fuel Oil': 'Petroleum'}

    df_list = []
    for f in fuels.keys():
        df = (source_fba.query(f'ActivityProducedBy == "{f} {sector}"')
              [['FlowName', 'FlowAmount']]
              .assign(Fuel=f)
              )
        df_list.append(df)
    # calculate ratio of flow to CO2 for each fuel (in CO2e)
    ratios = (pd.concat(df_list, ignore_index=True)
              .pivot_table(columns='FlowName',
                           index='Fuel',
                           values='FlowAmount')
              .assign(CH4=lambda x: x['CH4'] / x['CO2'])
              .assign(N2O=lambda x: x['N2O'] / x['CO2'])
              .drop(columns='CO2')
              .fillna(0)
              )

    # prepare dataframe from StateGHGI including CO2 flows by fuel type
    fba1 = (pd.concat([(
                           flowsa.flowbyactivity.getFlowByActivity('EPA_StateGHGI', year)
                           .query('ActivityProducedBy in @activity_list')),
                      fba.copy()],
                     ignore_index=True)
           .assign(Fuel=lambda x: x['ActivityProducedBy']
                   .str.rsplit(' - ', n=1, expand=True)[1])
           )

    # Derive state CH4 and N2O emissions by fuel type using fuel specific ratios
    fba2 = (fba1.query('FlowName == "CO2"')
                .assign(Fuel=lambda x: x['Fuel'].replace(
                    dict((v,k) for k,v in fuels.items())))
                .merge(ratios.reset_index())
                .assign(CH4=lambda x: x['CH4'] * x['FlowAmount'])
                .assign(N2O=lambda x: x['N2O'] * x['FlowAmount'])
                .melt(id_vars=['Location', 'Fuel'],
                      value_vars=['CH4', 'N2O'],
                      var_name='FlowName')
                .pivot_table(columns='Fuel',
                             index=['Location', 'FlowName'],
                             values='value')
                )
    fba2 = pd.DataFrame(fba2).div(fba2.sum(axis=1), axis=0)

    # Maintain source flow amount, merge in state ratios by fuel type
    fba3 = (fba1.merge(fba2.reset_index())
                .melt(id_vars=[c for c in fba1 if c not in fuels.keys()],
                      value_vars=fuels.keys())
                .assign(Description=lambda x: x['variable'].replace(fuels))
                .assign(FlowAmount=lambda x: x['FlowAmount'] * x['value'])
                .drop(columns=['Fuel', 'variable', 'value'])
                )

    if round(fba3.FlowAmount.sum(), 6) != round(fba.FlowAmount.sum(), 6):
        log.warning('Error: totals do not match when splitting CH4 and N2O by '
                    'fuel type')

    new_fba = FlowByActivity(fba3)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba


def allocate_industrial_combustion(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_before_activity_sets. Calculate the percentage of fuel consumption
    captured in EIA MECS relative to national GHGI. Create new activities to
    distinguish those which use EIA MECS as allocation source and those that
    use alternate source.
    """
    from flowsa.data_source_scripts.EPA_GHGI import get_manufacturing_energy_ratios
    pct_dict = get_manufacturing_energy_ratios(fba.config.get('clean_parameter'))

    # activities reflect flows in A_14 and 3_8 and 3_9
    alist = fba.config.get('clean_parameter')['activities_to_split']
    activities_to_split = {a: a.rsplit(' - ')[-1] for a in alist}

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


def drop_negative_values(fbs: FlowBySector, **_) -> FlowBySector:
    ## In some cases, after handling adjustments for reassigning emissions in
    ## the StateGHGI, sectors can have negative emissions after aggregating by
    ## sector. Remove these negative values so that that state does not get
    ## any emissions from that sector. clean_fbs_after_aggregation fxn
    fbs = fbs.query('FlowAmount >= 0').reset_index(drop=True)

    return fbs


if __name__ == '__main__':
    import flowsa
    flowsa.generateflowbyactivity.main(source='EPA_StateGHGI', year='2012-2022')
    fba = flowsa.flowbyactivity.getFlowByActivity('EPA_StateGHGI', '2020')
