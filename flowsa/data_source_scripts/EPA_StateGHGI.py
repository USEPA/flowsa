# EPA_StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US GHGs from EPA disaggregated to States
"""
import json
import pandas as pd
from flowsa.settings import externaldatapath
from flowsa.location import apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
import flowsa.exceptions


def epa_state_ghgi_parse(*, source, year, config, **_):

    try:
        with open(externaldatapath + config.get('file')) as f:
            data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError('State GHGI data not yet available for '
                                'external users')

    data_df = pd.DataFrame(data)
    activity_cols = ['SECTOR', 'SOURCE', 'SUBSOURCE', 'FUEL_TYPE',
                     'SUB_REFERENCE', 'SECSUB_REFERENCE']

    states = data_df[['STATE']].drop_duplicates()
    flows = data_df[['GHG_NAME']].drop_duplicates()

    df = data_df.melt(id_vars = activity_cols + ['STATE'] + ['GHG_NAME'],
                      value_vars=f'EMISSION_{year}',
                      var_name = 'Year',
                      value_name = 'FlowAmount')
    df['Year'] = year
    df['Unit'] = 'MMT CO2e' # TODO confirm units
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['SourceName'] = source
    df['Class'] = 'Chemicals'
    df['Compartment'] = 'air'

    df.rename(columns={'STATE': 'State',
                       'GHG_NAME': 'FlowName'},
              inplace=True)

    df['ActivityProducedBy'] = (df[activity_cols]
                                .apply(lambda row: ' - '.join(
                                      row.values.astype(str)), axis=1))
    df['ActivityProducedBy'] = (df['ActivityProducedBy']
                                .str.replace(' - None', ''))
    df.drop(columns=activity_cols, inplace=True)
    activities = df[['ActivityProducedBy']].drop_duplicates()

    df['County'] = ''
    df = apply_county_FIPS(df)
    df = assign_fips_location_system(df, '2015')
    df.drop(columns=['County'], inplace=True)

    return df


def remove_select_states(fba, source_dict, **_):
    """
    clean_fba_df_fxn to remove selected states so they can be added
    from alternate sources. State abbreviations must be passed as list
    in method parameter 'state_list'

    :param fba: df
    :param source_dict: dictionary of source methods includes 'state_list'
        key of states to remove
    """
    state_list = source_dict.get('state_list')
    state_df = pd.DataFrame(state_list, columns=['State'])
    state_df['County'] =''
    state_df = apply_county_FIPS(state_df)
    df_subset = fba[~fba['Location'].isin(state_df['Location'])]
    return df_subset


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


if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='EPA_StateGHGI', year='2017')
    fba = flowsa.getFlowByActivity('EPA_StateGHGI', '2017')
