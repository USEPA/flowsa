from flowsa.data_source_scripts import EPA_GHGI as ghgi
from flowsa.flowby import FlowByActivity


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
