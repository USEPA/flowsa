from flowsa import naics
from flowsa.flowsa_log import log
import pandas as pd
from flowsa.data_source_scripts import EIA_MECS as mecs
from flowsa.data_source_scripts import EPA_GHGI as ghgi
from flowsa.flowby import FlowByActivity


def clean_qcew(fba: FlowByActivity, **kwargs):
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')

    totals = (
        fba
        .query('ActivityProducedBy.str.len() == 3')
        [['Location', 'ActivityProducedBy', 'FlowAmount']]
        .assign(ActivityProducedBy=lambda x: (x.ActivityProducedBy
                                              .str.slice(stop=2)))
        .groupby(['Location', 'ActivityProducedBy']).agg('sum')
        .reset_index()
        .rename(columns={'FlowAmount': 'new_total'})
    )

    merged = fba.merge(totals, how='left')

    fixed = (
        merged
        .assign(FlowAmount=merged.FlowAmount.mask(
            (merged.ActivityProducedBy.str.len() == 2)
            & (merged.FlowAmount == 0),
            merged.new_total
        ))
        .drop(columns='new_total')
        .reset_index(drop=True)
    )

    target_naics = set(naics.industry_spec_key(fba.config['industry_spec'])
                       .target_naics)
    filtered = fixed.query('ActivityProducedBy in @target_naics')

    return filtered


def clean_usda_cropland_naics(fba: FlowByActivity, **kwargs):
    if fba.config['industry_spec']['default'] == 'NAICS_2':
        naics_2 = (
            fba
            .query('ActivityProducedBy.str.len() == 3')
            .assign(ActivityProducedBy=lambda x: (x.ActivityProducedBy
                                                  .str.slice(stop=2)))
            .groupby(fba.groupby_cols).agg('sum')
            .reset_index()
        )
        fba = pd.concat([naics_2, fba]).reset_index(drop=True)

    target_naics = set(naics.industry_spec_key(fba.config['industry_spec'])
                       .target_naics)
    filtered = fba.query('ActivityProducedBy in @target_naics')

    return filtered


def clean_mecs_energy_fba_for_bea_summary(fba: FlowByActivity, **kwargs):
    naics_3 = fba.query('ActivityConsumedBy.str.len() == 3')
    naics_4 = fba.query('ActivityConsumedBy.str.len() == 4 '
                        '& ActivityConsumedBy.str.startswith("336")')
    naics_4_sum = (
        naics_4
        .assign(ActivityConsumedBy='336')
        .aggregate_flowby()
        [['Flowable', 'FlowAmount', 'Unit', 'ActivityConsumedBy']]
        .rename(columns={'FlowAmount': 'naics_4_sum'})
    )

    merged = naics_3.merge(naics_4_sum, how='left').fillna({'naics_4_sum': 0})
    subtracted = (
        merged
        .assign(FlowAmount=merged.FlowAmount - merged.naics_4_sum)
        .drop(columns='naics_4_sum')
    )

    subtracted.config['naics_4_list'] = list(
        naics_4.ActivityConsumedBy.unique()
    )

    return subtracted


def clean_mapped_mecs_energy_fba_for_bea_summary(
    fba: FlowByActivity,
    **kwargs
):
    naics_4_list = fba.config['naics_4_list']

    return fba.query('~(SectorConsumedBy in @naics_4_list '
                     '& ActivityConsumedBy != SectorConsumedBy)')


def clean_hfc_fba(fba: FlowByActivity, **kwargs):
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    df = ghgi.clean_HFC_fba(fba)

    new_fba = FlowByActivity(df)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba


def split_hfcs_by_type(fba: FlowByActivity, **kwargs):
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    df = ghgi.split_HFCs_by_type(fba)

    new_fba = FlowByActivity(df)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba
