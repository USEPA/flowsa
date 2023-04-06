# EPA_StateGHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US GHGs from EPA disaggregated to States
"""
import pandas as pd
from flowsa.settings import externaldatapath
from flowsa.location import apply_county_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
import flowsa.exceptions


def epa_state_ghgi_parse(*, source, year, config, **_):

    try:
        data_df = pd.read_excel(externaldatapath + config.get('file'),
                                sheet_name='Data by Econ Sect')
    except FileNotFoundError:
        raise FileNotFoundError('State GHGI data not yet available for '
                                'external users')

    activity_cols = ['ECON_SECTOR', 'ECON_SOURCE', 'SUBSECTOR',
                     'CATEGORY', 'FUEL', 'SUBCATEGORY1',
                     'SUBCATEGORY2', 'SUBCATEGORY3', 'SUBCATEGORY4']

    states = data_df[['STATE']].drop_duplicates()
    flows = data_df[['GHG']].drop_duplicates()

    df = (data_df.melt(id_vars = activity_cols + ['STATE'] + ['GHG'],
                       value_vars=f'Y{year}',
                       var_name = 'Year',
                       value_name = 'FlowAmount')
                .assign(Year = year)
                .assign(Unit = 'MMT CO2e') # TODO confirm units
                .assign(FlowType = 'ELEMENTARY_FLOW')
                .assign(SourceName = source)
                .assign(Class = 'Chemicals')
                .assign(Compartment = 'air')
                .rename(columns={'STATE': 'State',
                                 'GHG': 'FlowName'})
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


if __name__ == '__main__':
    import flowsa
    flowsa.flowbyactivity.main(source='EPA_StateGHGI', year='2017')
    fba = flowsa.getFlowByActivity('EPA_StateGHGI', '2017')
