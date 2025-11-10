# necessary so 'FlowBySector'/'FlowByActivity' can be used in fxn
# annotations without importing the class to the py script which would lead
# to circular reasoning
from __future__ import annotations

import numpy as np
import pandas as pd
from flowsa.flowby import FB, get_flowby_from_config
from flowsa.common import get_catalog_info
from flowsa.flowsa_log import log
from flowsa import (geo, location)
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbysector import FlowBySector
from flowsa.naics import map_source_sectors_to_more_aggregated_sectors
from flowsa.validation import compare_summation_at_sector_lengths_between_two_dfs


def return_primary_flow_column(self: FB, flowtype='FBA') -> FB:
    """
    Determine activitiy column with values
    :param self: fbs df with two sector columns
    :param flowtype: str, 'FBA' or 'FBS'
    :return: string, primary sector column
    """
    flow = 'Activity'
    if flowtype == 'FBS':
        flow = 'Sector'
    if self[f'{flow}ProducedBy'].isnull().all():
        primary_column = f'{flow}ConsumedBy'
    elif self[f'{flow}ConsumedBy'].isnull().all():
        primary_column = f'{flow}ProducedBy'
    else:
        log.error(f'Could not determine primary {flow} column as there are '
                  f'values in both {flow}ProducedBy and {flow}ConsumedBy')
    return primary_column


def load_prepare_clean_source(
        self: 'FB',
        download_sources_ok: bool = True
    ) -> 'FB':
    """
    Add doc string
    """
    try:
        (name, config), = self.config['clean_source'].items()
    except AttributeError:
        name, config = self.config['clean_source'], {}

    clean_fbs = get_flowby_from_config(
        name=name,
        config={**{k: v for k, v in self.config.items()
                   if k in self.config['method_config_keys']
                   or k == 'method_config_keys'},
                **get_catalog_info(name),
                **config},
        download_sources_ok=download_sources_ok
        ).prepare_fbs(download_sources_ok=download_sources_ok)
    return clean_fbs.reset_index(drop=True)


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
              )
    merged['FlowAmount_other'] = merged['FlowAmount_other'].mask(
        merged['FlowAmount_other'] == 0, merged['FlowAmount'])

    # drop rows where flow is 0
    merged = merged[merged['FlowAmount'] != 0]
    # replace terms
    for original, replacement in fba.config.get(
            'replacement_dictionary').items():
        with pd.option_context('future.no_silent_downcasting', True):
            merged = (merged
                  .replace({original: replacement})
                  .infer_objects(copy=False)
                  )

    wt_flow = (
        merged
        .groupby(['Class', 'Flowable', 'Unit', 'FlowType', 'ActivityProducedBy',
                  'ActivityConsumedBy', 'Context', 'Location', 'LocationSystem',
                  'Year', 'MeasureofSpread', 'Spread', 'DistributionType', 'Min',
                  'Max', 'DataReliability', 'DataCollection', 'SectorProducedBy',
                  'ProducedBySectorType', 'SectorConsumedBy', 'ConsumedBySectorType',
                  'SectorSourceName'], dropna=False)[['FlowAmount', 'FlowAmount_other']]
        .apply(lambda x: np.average(x['FlowAmount'], weights=x['FlowAmount_other']))
        .reset_index(name='FlowAmount')
    )

    # set attributes todo: revise above code so don't lose attributes
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    for attr in attributes_to_save:
        setattr(wt_flow, attr, attributes_to_save[attr])

    # reset dropped information
    wt_flow = (wt_flow
               .reset_index(drop=True)
               .reset_index(names='group_id')
               .assign(group_total=wt_flow.FlowAmount)
               )

    return wt_flow


def substitute_nonexistent_values(
        fb: 'FB',
        download_sources_ok: bool = True,
        **kwargs
    ) -> 'FB':
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
    ]).reset_index(drop=True)

    other = (other
             .merge(state_geo)
             .drop(columns=['Location', 'FlowUUID'])
             .rename(columns={'FIPS': 'Location'})
             )

    # todo: revise these check merge cols, expand
    merged = (fb
              .merge(other,
                     on=list(other.select_dtypes(
                         include=['object', 'int']).columns),
                     how='outer',
                     suffixes=(None, '_y'))
              .fillna({'FlowAmount': 0})
              )
    # fill in missing data
    new_col_data = [col for col in merged if col.endswith('_y')]
    for c in new_col_data:
        original_col = c.replace('_y', '')
        if merged[original_col].dtype != 'string':
            merged[original_col] = np.where(merged[original_col] == 0,
                                            merged[c],
                                            merged[original_col])
        else:
            merged[original_col] = merged[original_col].fillna(
                merged[c])

    # reset group id and group total, drop columns
    merged = (merged
              .drop(merged.filter(regex='_y').columns, axis=1)
              .drop(columns=['group_id'])
              .reset_index(drop=True)
              .reset_index(names='group_id')
              .assign(group_total=merged.FlowAmount)
              )

    merged_null = merged[merged['FlowAmount'] == 0]
    if len(merged_null) > 0:
        log.warning('Not all null values were substituted')

    return merged


def estimate_suppressed_sectors_equal_attribution(
        fba: FlowByActivity) -> FlowByActivity:
    """
    Method to estimate suppressed data only works for activity-like sectors
    :param fba:
    :return:
    """
    from flowsa.naics import map_source_sectors_to_less_aggregated_sectors
    # todo: update function to work for any number of sector lengths
    # todo: update to loop through both sector columns, see equally_attribute()

    log.info('Estimating suppressed data by equally attributing parent to '
             'child sectors.')
    naics_key = map_source_sectors_to_more_aggregated_sectors(
        year=fba.config['target_naics_year'])
    # forward fill
    naics_key = naics_key.T.ffill().T

    col = return_primary_flow_column(fba, 'FBA')

    # determine if there are any 1:1 parent:child sectors that are missing,
    # if so, add them (true for usda_coa_cropland_naics df)
    cw_melt = map_source_sectors_to_less_aggregated_sectors(
        fba.config['target_naics_year'])
    cw_melt = cw_melt.assign(count=(cw_melt
                                    .groupby(['source_naics', 'SectorLength'])
                                    ['source_naics']
                                    .transform('count')))
    cw = (cw_melt
          .query("count==1")
          .drop(columns=['SectorLength', 'count'])
          )
    # create new df with activity col values reassigned to their child sectors
    fba2 = (fba
            .merge(cw, left_on=col, right_on='source_naics', how='left')
            .assign(**{f"{col}": lambda x: x.Sector})
            .drop(columns=['source_naics', 'Sector'])
            .query(f"~{col}.isna()")
            .drop_duplicates()  # duplicates if multiple generations of 1:1
            )
    # merge the two dfs - add the child sectors to original df when there is
    # only single parent:child
    fba3 = fba.merge(fba2, how='outer')

    fba3 = (fba3
            .assign(Unattributed=fba3.FlowAmount.copy(),
                    Attributed=0)
            .assign(descendants='')
            )

    # drop rows that contain "&" and "-"
    fba3 = (fba3
            .query(f"~{col}.str.contains('&')")
            .query(f"~{col}.str.contains('-')")
            )

    for level in [6, 5, 4, 3, 2]:
        descendants = (
            fba3
            .drop(columns='descendants')
            .query(f'{col}.str.len() > {level}')
            .assign(
                parent=lambda x: x[col].str.slice(stop=level)
            )
            .groupby(['FlowName', 'Location', 'parent'])
            .agg({'Unattributed': 'sum', col: ' '.join})
            .reset_index()
            .rename(columns={col: 'descendants',
                             'Unattributed': 'descendant_flows',
                             'parent': col})
        )
        fba3 = (
            fba3
            .merge(descendants,
                   how='left',
                   on=['FlowName', 'Location', col],
                   suffixes=(None, '_y'))
            .fillna({'descendant_flows': 0, 'descendants_y': ''})
            .assign(
                descendants=lambda x: x.descendants.mask(x.descendants == '',
                                                         x.descendants_y),
                Unattributed=lambda x: (x.Unattributed -
                                        x.descendant_flows).mask(
                    x.Unattributed - x.descendant_flows < 0, 0),
                Attributed=lambda x: (x.Attributed +
                                      x.descendant_flows)
            )
            .drop(columns=['descendant_flows', 'descendants_y'])
        )

    fba3 = fba3.drop(columns=['descendants'])

    # todo: All hyphenated sectors are currently dropped, modify code so
    #  they are not
    fba_m = (
        fba3
        .merge(naics_key, how='left', left_on=col,
               right_on='source_naics')
        .assign(location=fba3.Location,
                category=fba3.FlowName)
        # .replace({'FlowAmount': {0: np.nan}  #,
                  # col: {'1125 & 1129': '112X',
                  #       '11193 & 11194 & 11199': '1119X',
                  #       '31-33': '3X',
                  #       '44-45': '4X',
                  #       '48-49': '4Y'},
                  # 'n2': {'31': '3X', '32': '3X', '33': '3X',
                  #        '44': '4X', '45': '4X',
                  #        '48': '4Y', '49': '4Y'},
                  # 'n4': {'1125': '112X', '1129': '112X'},
                  # 'n5': {'11193': '1119X', '11194': '1119X', '11199': '1119X'}
                  # })
        .dropna(subset='source_naics')
        .drop(columns='source_naics')
    )

    indexed = fba_m.set_index(['n2', 'n3', 'n4', 'n5', 'n6', 'n7',
                               'location', 'category'], verify_integrity=True)

    def fill_suppressed(
            flows, level: int, activity
    ):
        parent = flows[flows[activity].str.len() == level]
        children = flows[flows[activity].str.len() == level + 1]
        null_children = children[children['flow_suppressed']]

        if null_children.empty or parent.empty:
            return flows
        else:
            value = max(parent['Unattributed'].iloc[0] / len(null_children), 0)
            # update the null children by adding the unattributed data to
            # the attributed data
            null_children = (
                null_children
                .assign(FlowAmount=value+null_children['Attributed'])
                .assign(Unattributed=value)
            )
            flows.loc[null_children.index, ['FlowAmount', 'Unattributed']] = null_children[['FlowAmount', 'Unattributed']]
            return flows

    unsuppressed = indexed.copy()
    # replace 0 values with np.nan for suppressed data to be estimated
    unsuppressed['FlowAmount'] = unsuppressed['FlowAmount'].mask(unsuppressed['FlowAmount'] == 0)
    unsuppressed['flow_suppressed'] = unsuppressed['FlowAmount'].isna()

    # loop through sector lengths, estimating suppressed data
    for level in [2, 3, 4, 5, 6]:
        log.info(f"Estimating suppressed data at sector level {level}")
        groupcols = (["{}{}".format("n", i) for i in range(2, level+1)] +
                     ['location', 'category'])
        unsuppressed = (unsuppressed
                        .groupby(level=groupcols)
                        .apply(fill_suppressed, level, col)
                        )
    unsuppressed['Year'] = unsuppressed['Year'].astype('int')

    aggregated = (
        unsuppressed
        .reset_index(drop=True)
        .fillna({'FlowAmount': 0})
        .drop(columns=['Unattributed', 'Attributed', 'flow_suppressed'])
        # .replace({col: {'3X': '31-33',
        #                 '4X': '44-45',
        #                 '4Y': '48-49'}})
        .aggregate_flowby()
    )

    compare_summation_at_sector_lengths_between_two_dfs(fba, aggregated)

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
    hlp = load_prepare_clean_source(fba)

    # To match the sector resolution of source data, generate employment
    # dataset for all NAICS resolution by aggregating
    hlp = hlp.aggregate_flowby()

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
        .reset_index(drop=True)
        .reset_index(names='group_id')
        .assign(group_total=fba.FlowAmount)
    )

    # Check for data loss
    if (abs(1-(sum(fba['FlowAmount']) /
               sum(fba_load['FlowAmount'])))) > 0.0005:
        log.warning('Data loss upon census region mapping')

    return fba

def calculate_flow_per_person(
        fbs: 'FlowBySector',
        download_sources_ok: bool = True,
        **_
    ) -> 'FlowBySector':
    """
    Calculates FlowAmount per person (or other metric) per year based on
    dataset name passed in "clean_parameter"
    clean_fbs function
    """
    bls = load_prepare_clean_source(fbs,
                                    download_sources_ok=download_sources_ok)
    cols = ['Location', 'Year', 'SectorProducedBy']
    if bls['SectorProducedBy'].isna().all():
        bls = bls.assign(SectorProducedBy = bls['SectorConsumedBy'])
    ## TODO ^^ need to account for mismatched of ProducedBy/ConsumedBy
    fbs = (fbs
           .sector_aggregation()
           .aggregate_flowby()
           # ^^ handles updated industry specs
           .merge(bls
                  .rename(columns={'FlowAmount': 'persons'})
                  .groupby(cols).agg({'persons': 'sum'})
                  .reset_index(),
                  how='inner',
                  on=cols)
           .assign(FlowAmount=lambda x: np.divide(
        x['FlowAmount'], x['persons'], out=np.zeros_like(
            x['persons']), where=x['persons'] != 0))
            .assign(Unit = lambda x: x['Unit'] + '/p')
            .drop(columns=['persons'])
            )

    return fbs


def define_parentincompletechild_descendants(
        fba: FlowByActivity, activity_col='ActivityConsumedBy', **_) -> \
        FlowByActivity:
    '''
    This function helps address the unique structure of the EIA MECS dataset.
    The MECS dataset contains rows at various levels of aggregation between
    NAICS-3 and NAICS-6 (inclusive). Each aggregated row contains the total
    for that level of aggregation, even if data are also reported for a less
    aggregated subset of those industries. For example:

    ActivityConsumedBy | FlowAmount | ...
    -------------------------------------
    311                | 110        |
    3112               |  65        |
    311221             |  55        |

    where the 110 reported for 311 includes the 65 reported for 3112, which
    includes the 55 reported for 211221. If we do not address this issue, there
    will be double counting. Additionally, if we are trying to disaggregate
    to the NAICS-6 level, all three rows shown above will be mapped to NAICS-6
    311221 (with the first wo rows also being mapped to several other NAICS-6
    codes as well). We will then over attribute the (double-counted) flows to
    those industries and groups of industries for which more specific detail
    is provided.

    This function addresses the double counting issue. For each aggregated
    industry group, all descendant (less aggregated) industries or industry
    groups for which detailed information is given are subtracted from the
    aggregated total. Using the example from above:

    ActivityConsumedBy | FlowAmount | ...
    -------------------------------------
    311                |  45        |
    3112               |  10        |
    311221             |  55        |

    Additionally, this function adds a column called "descendants", which for
    each industry holds all the descendant industries or industry groups that
    have detailed information provided in the dataset. After mapping to
    industries, but before attribution is performed, this column is used by the
    drop_parentincompletechild_descendants function to drop any row that is mapped
    from an aggregated industry group to a less aggregated industry or industry
    group THAT HAS DETAILED INFORMATION GIVEN IN THE MECS (and therefore has
    its own row already) to avoid the over-attribution issue.
    Again using the previous example:

    ActivityConsumedBy | FlowAmount | descendants | ...
    ---------------------------------------------------
    311                |  45        | 3112 311221 |
    3112               |  10        | 311221      |
    311221             |  55        |             |

    Note that this function is not useful if the desired aggregation level is
    NAICS-2. In such a case, the MECS dataset can be filtered to include only
    the rows with ActivityConsumedBy == "31-33", then disaggregated to 31, 32,
    33 using another dataset (such as the QCEW).
    '''
    fba = (
        fba
        .query(f'{activity_col} != "31-33"')
        .assign(descendants='')
    )

    for level in [5, 4, 3]:
        descendants = (
            fba
            .drop(columns='descendants')
            .query(f'{activity_col}.str.len() > {level}')
            .assign(
                parent=lambda x: x[activity_col].str.slice(stop=level)
            )
            .groupby(['Flowable', 'Location', 'parent'])
            .agg({'FlowAmount': 'sum', activity_col: ' '.join})
            .reset_index()
            .rename(columns={activity_col: 'descendants',
                             'FlowAmount': 'descendant_flows',
                             'parent': activity_col})
        )

        fba = (
            fba
            .merge(descendants,
                   how='left',
                   on=['Flowable', 'Location', activity_col],
                   suffixes=(None, '_y'))
            .fillna({'descendant_flows': 0, 'descendants_y': ''})
            .assign(
                descendants=lambda x: x.descendants.mask(x.descendants == '',
                                                         x.descendants_y),
                FlowAmount=lambda x: (x.FlowAmount -
                                      x.descendant_flows).mask(
                    x.FlowAmount - x.descendant_flows < 0, 0)
            )
            .drop(columns=['descendant_flows', 'descendants_y'])
        )
    # Reset group_total after adjusting for descendents
    fba = (fba
           .drop(columns='group_total')
           .merge((fba.groupby('group_id')
                      .agg({'FlowAmount': "sum"})
                      .rename(columns={'FlowAmount': 'group_total'})
                      ),
                  on='group_id', how='left', validate='m:1')
           )

    return fba


def drop_parentincompletechild_descendants(
        fba: FlowByActivity, sector_col='SectorConsumedBy', **_) -> \
        FlowByActivity:
    '''
    This function finishes handling the over-attribution issue described in
    the documentation for define_parentincompletechild_descendants by dropping any row in the
    MECS dataset which has been mapped to an industry or industry group which
    is a subset (strict or otherwise) of an industry group listed in the
    descendants columns. So, if 311 and 3112 both appear in the MECS datset,
    3112 will be listed as a descendant of 311 and this function will therefore
    drop a row mapping 311 to 311221 (since more detailed information on 3112,
    which contains 311221, is provided). If 31122 and 311221 do not appear in
    the dataset, a row mapping 3112 to 311221 will not be dropped, since no
    more detailed information on 311221 is given. Further attribution/
    disaggregation should be done using another datatset such as the QCEW.
    '''

    fba2 = (
        fba
        .assign(to_keep=fba.apply(
            lambda x: not any([str(x[sector_col]).startswith(d) for d in
                               x.descendants.split()]),
            axis='columns'
        ))
        .query('to_keep')
        .drop(columns=['descendants', 'to_keep'])
    )

    return fba2


def proxy_sector_data(
        fba: 'FlowByActivity',
        download_sources_ok: bool = True,
        **kwargs
) -> 'FlowByActivity':
    """
    Use a dictionary to use data for one sector as proxy data for a second
    sector.

    To implement, use in an FBS method
    attribution_method: direct
    # equate the water application rates of strawberries to other berries
    clean_fba_after_attribution: !clean_function:flowbyclean proxy_sector_data
    proxy_sectors: {'111334': '111333'}

    :param fba:
    :param download_sources_ok:
    :param kwargs:
    :return:
    """
    col = return_primary_flow_column(fba, flowtype='FBS')
    proxy = fba.config['proxy_sectors']

    fba2 = fba.drop(columns='group_id')
    for k, v in proxy.items():
        fba2[col] = np.where(fba2[col] == k, f'{k},{v}', fba2[col])
    # convert sector col to list
    fba2[col] = fba2[col].str.split(",")
    # break each sector into separate line
    fba3 = (fba2
            .explode(col)
            .reset_index(drop=True)
            .reset_index(names='group_id')
            )

    return fba3
