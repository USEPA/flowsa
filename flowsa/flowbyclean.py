import numpy as np
import pandas as pd
from flowsa.flowby import FlowByActivity, FlowBySector
from flowsa.flowsa_log import log
from flowsa import (flowby, geo, location)



def load_prepare_clean_source(
        self: 'FlowByActivity'
) -> 'FlowBySector':

    (name, config), = self.config['clean_source'].items()

    clean_fbs = flowby.get_flowby_from_config(
        name=name,
        config={**{k: v for k, v in self.config.items()
                   if k in self.config['method_config_keys']
                   or k == 'method_config_keys'},
                **flowby.get_catalog_info(name),
                **config}
        ).prepare_fbs()
    return clean_fbs


def weighted_average(
        fba: 'FlowByActivity',
        **kwargs
) -> 'FlowByActivity':
    """
    This method determines weighted average
    """

    # load secondary FBS
    other = load_prepare_clean_source(fba)

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
              .fillna({'FlowAmount_other': 0})
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
        fba: 'FlowByActivity',
        **kwargs
) -> 'FlowByActivity':
    """
    This method determines weighted average
    """

    # load secondary FBS
    other = load_prepare_clean_source(fba)
    other = (other
             .add_primary_secondary_columns('Sector')
             .drop(columns=['MetaSources', 'AttributionSources']))

    log.info('Substituting nonexistent values in %s with %s.',
             fba.full_name, other.full_name)

    fba = (fba
           .add_primary_secondary_columns('Sector')
           )

    # merge all possible national data with each state
    state_geo = pd.concat([
        (geo.filtered_fips(fba.config['geoscale'])[['FIPS']]
         .assign(Location=location.US_FIPS))
    ])

    other = (other
             .merge(state_geo)
             .drop(columns=['Location', 'FlowUUID'])
             .rename(columns={'FIPS': 'Location',
                              'FlowAmount': 'FlowAmount_other'})
             )

    merged = (fba
              .merge(other,
                     on=list(other.select_dtypes(
                         include=['object', 'int']).columns),
                     how='outer')
              .assign(FlowAmount=lambda x: x.FlowAmount.
                      fillna(x.FlowAmount_other))
              .drop(columns=['PrimarySector', 'SecondarySector',
                             'FlowAmount_other', 'group_id'],
                    errors='ignore')
              .reset_index(drop=True).reset_index()
              .rename(columns={'index': 'group_id'})
              )
    # replace float dtypes with new data
    merged = (merged
              .drop(merged.filter(regex='_x').columns, axis=1)
              .rename(columns=lambda x: x.replace('_y', ''))
              .assign(group_total=merged.FlowAmount)
              )

    merged_null = merged[merged['FlowAmount'] == 0]
    if len(merged_null) > 0:
        log.warning('Not all null values were substituted')

    return merged
