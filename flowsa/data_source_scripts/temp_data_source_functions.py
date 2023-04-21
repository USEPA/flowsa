from flowsa import naics
from flowsa import settings
from flowsa.flowsa_log import log
import pandas as pd
import numpy as np
from flowsa.data_source_scripts import EPA_GHGI as ghgi
from flowsa.data_source_scripts import USDA_CoA_Cropland as coa
from flowsa.flowby import FlowByActivity


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

    target_naics = set(
        naics.industry_spec_key(fba.config['industry_spec'])
        .target_naics
        .str.replace('0', '')
    ) | {'1122', '1125'}

    filtered = fba.query('ActivityConsumedBy in @target_naics')

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


def clean_hfc_fba_for_seea(fba: FlowByActivity, **kwargs):
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    df = (
        fba
        .pipe(ghgi.subtract_HFC_transport_emissions)
        .pipe(ghgi.allocate_HFC_to_residential)
        .pipe(ghgi.split_HFC_foams)
    )

    new_fba = FlowByActivity(df)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba


def disaggregate_coa_cropland_to_6_digit_naics(fba: FlowByActivity):
    """
    Disaggregate usda coa cropland to naics 6. Fragile implementation, should
    be replaced. In particular, it will break things for any industry
    specification other than {'default': 'NAICS_6'}.
    :param fba: df, CoA cropland data, FBA format with sector columns
    :return: df, CoA cropland with disaggregated NAICS sectors
    """
    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    df = coa.disaggregate_coa_cropland_to_6_digit_naics(
        fba, fba.config, fba.config,
        download_FBA_if_missing=settings.DEFAULT_DOWNLOAD_IF_MISSING
    )

    new_fba = FlowByActivity(df)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba

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

def estimate_suppressed_sectors_equal_attribution(fba: FlowByActivity) -> \
        FlowByActivity:

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
        .replace({'FlowAmount': {0: np.nan}})
        .set_index(['n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'location',
                    'category'], verify_integrity=True)
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
                .transform(fill_suppressed, 2, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 3, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 4, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4', 'n5',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 5, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4', 'n5', 'n6',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 6, x.ActivityProducedBy)))
        .fillna({'FlowAmount': 0})
        .reset_index(drop=True)
    )

    aggregated = (
        unsuppressed
        .aggregate_flowby()
    )

    return aggregated
