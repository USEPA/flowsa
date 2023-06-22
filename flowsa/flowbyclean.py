import numpy as np
import pandas as pd
from flowsa.flowby import FlowByActivity, FlowBySector
from flowsa.flowsa_log import log
from flowsa import (flowby, geo, location, getFlowBySector, flowbyfunctions)


def return_primary_activity_column(fba: FlowByActivity) -> \
        FlowByActivity:
    """
    Determine activitiy column with values
    :param fba: fbs df with two sector columns
    :return: string, primary sector column
    """
    if fba['ActivityProducedBy'].isnull().all():
        primary_column = 'ActivityConsumedBy'
    elif fba['ActivityConsumedBy'].isnull().all():
        primary_column = 'ActivityProducedBy'
    else:
        log.error('Could not determine primary activity column as there '
                  'are values in both ActivityProducedBy and '
                  'ActivityConsumedBy')
    return primary_column


def load_prepare_clean_source(
        self: 'FlowByActivity',
        download_sources_ok: bool = True
) -> 'FlowBySector':

    (name, config), = self.config['clean_source'].items()

    clean_fbs = flowby.get_flowby_from_config(
        name=name,
        config={**{k: v for k, v in self.config.items()
                   if k in self.config['method_config_keys']
                   or k == 'method_config_keys'},
                **flowby.get_catalog_info(name),
                **config},
        download_sources_ok=download_sources_ok
        ).prepare_fbs(download_sources_ok=download_sources_ok)
    return clean_fbs


def weighted_average(
        fba: 'FlowByActivity',
        download_sources_ok: bool = True,
        **kwargs
) -> 'FlowByActivity':
    """
    This method determines weighted average
    """

    # load secondary FBS
    other = load_prepare_clean_source(fba, download_sources_ok=download_sources_ok)

    log.info('Taking weighted average of %s by %s.',
             fba.full_name, other.full_name)

    fba_geoscale, other_geoscale, fba, other = fba.harmonize_geoscale(
        other)

    # merge dfs
    merged = (fba
              .merge(other,
                     how='left',
                     left_on=['PrimarySector', 'temp_location'
                     if 'temp_location' in fba
                     else 'Location'],
                     right_on=['PrimarySector', 'Location'],
                     suffixes=[None, '_other'])
              .fillna({'FlowAmount_other': fba['FlowAmount']})
              )
    # drop rows where flow is 0
    merged = merged[merged['FlowAmount'] != 0]
    # replace terms
    for original, replacement in fba.config.get(
            'replacement_dictionary').items():
        merged = merged.replace({original: replacement})

    wt_flow = (merged
               .groupby(['Class', 'Flowable', 'Unit',
                         'FlowType', 'ActivityProducedBy',
                         'ActivityConsumedBy', 'Context', 'Location',
                         'LocationSystem', 'Year', 'MeasureofSpread',
                         'Spread', 'DistributionType', 'Min', 'Max',
                         'DataReliability', 'DataCollection',
                         'SectorProducedBy', 'ProducedBySectorType',
                         'SectorConsumedBy', 'ConsumedBySectorType',
                         'SectorSourceName'],
                        dropna=False)
               .apply(lambda x: np.average(x['FlowAmount'],
                                           weights=x['FlowAmount_other']))
               .drop(columns='FlowAmount')  # original flowamounts
               .reset_index(name='FlowAmount')  # new, weighted flows
               )
    # set attributes todo: revise above code so don't lose attributes
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    for attr in attributes_to_save:
        setattr(wt_flow, attr, attributes_to_save[attr])

    # reset dropped information
    wt_flow = (wt_flow
               .reset_index(drop=True).reset_index()
               .rename(columns={'index': 'group_id'})
               .assign(group_total=wt_flow.FlowAmount)
               )

    return wt_flow


def substitute_nonexistent_values(
        fb: 'FlowBy',
        download_sources_ok: bool = True,
        **kwargs
) -> 'FlowBy':
    """
    Fill missing values with data from another geoscale
    """

    # load secondary FBS
    other = load_prepare_clean_source(fb, download_sources_ok)

    log.info('Substituting nonexistent values in %s with %s.',
             fb.full_name, other.full_name)

    # merge all possible national data with each state
    state_geo = pd.concat([
        (geo.filtered_fips(fb.config['geoscale'])[['FIPS']]
         .assign(Location=location.US_FIPS))
    ])

    other = (other
             .merge(state_geo)
             .drop(columns=['Location', 'FlowUUID'])
             .rename(columns={'FIPS': 'Location'})
             )

    merged = (fb
              .merge(other,
                     on=list(other.select_dtypes(
                         include=['object', 'int']).columns),
                     how='outer',
                     suffixes=(None, '_y'))
              )
    # fill in missing data
    new_col_data = [col for col in merged if col.endswith('_y')]
    for c in new_col_data:
        original_col = c.replace('_y', '')
        merged[original_col] = merged[original_col].fillna(
            merged[c])

    # reset grop id and group total, drop columns
    merged = (merged
              .drop(merged.filter(regex='_y').columns, axis=1)
              .drop(columns=['group_id'])
              .reset_index(drop=True).reset_index()
              .rename(columns={'index': 'group_id'})
              .assign(group_total=merged.FlowAmount)
              )

    merged_null = merged[merged['FlowAmount'] == 0]
    if len(merged_null) > 0:
        log.warning('Not all null values were substituted')

    return merged


def estimate_suppressed_sectors_equal_attribution(
        fba: FlowByActivity) -> FlowByActivity:
    """

    :param fba:
    :return:
    """

    col = return_primary_activity_column(fba)
    indexed = (
        fba
        .assign(n2=fba[col].str.slice(stop=2),
                n3=fba[col].str.slice(stop=3),
                n4=fba[col].str.slice(stop=4),
                n5=fba[col].str.slice(stop=5),
                n6=fba[col].str.slice(stop=6),
                n7=fba[col].str.slice(stop=7),
                location=fba.Location,
                category=fba.FlowName)
        .replace({'FlowAmount': {0: np.nan},
                  col: {'31-33': '3X',
                        '44-45': '4X',
                        '48-49': '4Y'},
                  'n2': {'31': '3X', '32': '3X', '33': '3X',
                         '44': '4X', '45': '4X',
                         '48': '4Y', '49': '4Y'}})
        .set_index(['n2', 'n3', 'n4', 'n5', 'n6', 'location', 'category'],
                   verify_integrity=True)
    )

    def fill_suppressed(
        flows: pd.Series,
        level: int,
        full_naics: pd.Series
    ) -> pd.Series:
        parent = flows[full_naics.str.len() == level]
        children = flows[full_naics.str.len() == level + 1]
        null_children = children[children.isna()]

        if null_children.empty or parent.empty:
            return flows
        else:
            value = max((parent[0] - children.sum()) / null_children.size, 0)
            return flows.fillna(pd.Series(value, index=null_children.index))

    unsuppressed = (
        indexed
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 2, x[col])))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 3, x[col])))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 4, x[col])))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4', 'n5',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 5, x[col])))
        .fillna({'FlowAmount': 0})
        .reset_index(drop=True)
    )

    aggregated = (
        unsuppressed
        .replace({col: {'3X': '31-33',
                        '4X': '44-45',
                        '4Y': '48-49'}})
        .aggregate_flowby()
    )

    return aggregated


def attribute_national_to_states(fba: FlowByActivity, **_) -> FlowByActivity:
    """
    Propogates national data to all states to enable for use in state methods.
    Allocates sectors across states based on employment.
    clean_allocation_fba_w_sec fxn
    """
    fba_load = fba.copy()
    log.info('Attributing national data to states')

    # Attribute data source based on attribution source
    # todo: generalize so works for FBA or FBS
    hlp = getFlowBySector(
        methodname=fba.config.get('clean_source'),
        download_FBS_if_missing=True)

    # To match the sector resolution of source data, generate employment
    # dataset for all NAICS resolution by aggregating
    hlp = flowbyfunctions.sector_aggregation(hlp)

    # For each region, generate ratios across states for a given sector
    hlp['Allocation'] = hlp['FlowAmount']/hlp.groupby(
        ['SectorProducedBy', 'SectorConsumedBy'],
        dropna=False).FlowAmount.transform('sum')

    # add column to merge on
    hlp = hlp.assign(Location_merge='00000')

    # todo: generalize so works for data sources other than employment FBS
    fba = pd.merge(
        fba.rename(columns={'Location': 'Location_merge'}),
        (hlp[['Location_merge', 'Location', 'SectorProducedBy', 'Allocation']]
         .rename(columns={'SectorProducedBy': 'SectorConsumedBy'})),
        how='left', on=['Location_merge', 'SectorConsumedBy'])
    fba = (fba.assign(FlowAmount=lambda x: x['FlowAmount'] * x['Allocation'])
           .drop(columns=['Allocation', 'Location_merge'])
           )

    # Rest group_id and group_total
    fba = (
        fba
        .drop(columns=['group_id', 'group_total'])
        .reset_index(drop=True).reset_index()
        .rename(columns={'index': 'group_id'})
        .assign(group_total=fba.FlowAmount)
    )

    # Check for data loss
    if (abs(1-(sum(fba['FlowAmount']) /
               sum(fba_load['FlowAmount'])))) > 0.0005:
        log.warning('Data loss upon census region mapping')

    return fba
