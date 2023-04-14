from flowsa import naics
from flowsa import settings
from flowsa.flowsa_log import log
import pandas as pd
import numpy as np
from flowsa.data_source_scripts import EPA_GHGI as ghgi
from flowsa.data_source_scripts import USDA_CoA_Cropland as coa
from flowsa.flowby import FlowByActivity
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def clean_qcew(fba: FlowByActivity, **kwargs):
    #todo: check function method for state
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
    filtered = (
        fixed
        .assign(ActivityProducedBy=fixed.ActivityProducedBy.mask(
            (fixed.ActivityProducedBy + '0').isin(target_naics),
            fixed.ActivityProducedBy + '0'
        ))
        .query('ActivityProducedBy in @target_naics')
    )

    return filtered


def clean_qcew_for_fbs(fba: FlowByActivity, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    fba['Flowable'] = 'Jobs'
    return fba


def estimate_suppressed_qcew(fba: FlowByActivity) -> FlowByActivity:
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')
    else:
        log.critical('At a subnational scale, this will take a long time.')

    indexed = (
        fba
        .assign(n2=fba.ActivityProducedBy.str.slice(stop=2),
                n3=fba.ActivityProducedBy.str.slice(stop=3),
                n4=fba.ActivityProducedBy.str.slice(stop=4),
                n5=fba.ActivityProducedBy.str.slice(stop=5),
                n6=fba.ActivityProducedBy.str.slice(stop=6),
                location=fba.Location,
                category=fba.FlowName)
        .replace({'FlowAmount': {0: np.nan},
                  'ActivityProducedBy': {'31-33': '3X',
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
        .fillna({'FlowAmount': 0})
        .reset_index(drop=True)
    )

    aggregated = (
        unsuppressed
        .assign(FlowName='Number of employees')
        .replace({'ActivityProducedBy': {'3X': '31-33',
                                         '4X': '44-45',
                                         '4Y': '48-49'}})
        .aggregate_flowby()
    )

    return aggregated


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


def eia_mecs_energy_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param source: source
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    from flowsa.location import assign_census_regions

    # concatenate dataframe list into single dataframe
    df = pd.concat(df_list, sort=True)

    # rename columns to match standard flowbyactivity format
    df = df.rename(columns={'NAICS Code': 'ActivityConsumedBy',
                            'Table Name': 'Description'})
    df.loc[df['Subsector and Industry'] == 'Total', 'ActivityConsumedBy'] = '31-33'
    df = df.drop(columns='Subsector and Industry')
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
    # add hardcoded data
    df["SourceName"] = source
    df["Compartment"] = None
    df['FlowType'] = 'TECHNOSPHERE_FLOWS'
    df['Year'] = year
    df['MeasureofSpread'] = "RSE"
    # assign location codes and location system
    df.loc[df['Location'] == 'Total United States', 'Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df = assign_census_regions(df)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # drop rows that reflect subtotals (only necessary in 2014)
    df.dropna(subset=['ActivityConsumedBy'], inplace=True)

    suppressed = df.assign(
        FlowAmount=df.FlowAmount.mask(df.FlowAmount.str.isnumeric() == False,
                                      np.nan),
        Suppressed=df.FlowAmount.where(df.FlowAmount.str.isnumeric() == False,
                                       np.nan),
        Spread=df.Spread.mask(df.Spread.str.isnumeric() == False, np.nan)
    )

    return suppressed


def estimate_suppressed_mecs_energy(
    fba: FlowByActivity,
    **kwargs
) -> FlowByActivity:
    '''
    Rough first pass at an estimation method, for testing purposes. This
    will drop rows with 'D' or 'Q' values, on the grounds that as far as I can
    tell we don't have any more information for them than we do for any
    industry without its own line item in the MECS anyway. '*' is for value
    less than 0.5 Trillion Btu and will be assumed to be 0.25 Trillion Btu
    '''
    if 'Suppressed' not in fba.columns:
        log.warning('The current MECS dataframe does not contain data '
                    'on estimation method and so suppressed data will '
                    'not be assessed.')
        return fba
    dropped = fba.query('Suppressed not in ["D", "Q"]')
    unsuppressed = dropped.assign(
        FlowAmount=dropped.FlowAmount.mask(dropped.Suppressed == '*', 0.25)
    )

    return unsuppressed.drop(columns='Suppressed')


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
