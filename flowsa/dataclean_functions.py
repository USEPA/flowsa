"""

"""

import logging as log
import sys
import flowsa
from flowsa.common import flow_by_activity_fields
from flowsa.datachecks import check_if_data_exists_at_geoscale
from flowsa.flowbyfunctions import clean_df, fba_fill_na_dict, harmonize_units, subset_df_by_geoscale
from flowsa.mapping import add_sectors_to_flowbyactivity


# import specific functions
from flowsa.data_source_scripts.BEA import subset_BEA_Use
from flowsa.data_source_scripts.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, convert_blackhurst_data_to_gal_per_employee
from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba, clean_bls_qcew_fba_for_employment_sat_table, \
    bls_clean_allocation_fba_w_sec
from flowsa.data_source_scripts.EIA_CBECS_Land import cbecs_land_fba_cleanup
from flowsa.data_source_scripts.EIA_MECS import mecs_energy_fba_cleanup, eia_mecs_energy_clean_allocation_fba_w_sec, \
    mecs_land_fba_cleanup, mecs_land_fba_cleanup_for_land_2012_fbs, mecs_land_clean_allocation_mapped_fba_w_sec
from flowsa.data_source_scripts.EPA_NEI import clean_NEI_fba, clean_NEI_fba_no_pesticides
from flowsa.data_source_scripts.StatCan_IWS_MI import convert_statcan_data_to_US_water_use, disaggregate_statcan_to_naics_6
from flowsa.data_source_scripts.stewiFBS import stewicombo_to_sector, stewi_to_sector
from flowsa.data_source_scripts.USDA_CoA_Cropland import disaggregate_coa_cropland_to_6_digit_naics, coa_irrigated_cropland_fba_cleanup
from flowsa.data_source_scripts.USDA_ERS_MLU import allocate_usda_ers_mlu_land_in_urban_areas, allocate_usda_ers_mlu_other_land,\
    allocate_usda_ers_mlu_land_in_rural_transportation_areas
from flowsa.data_source_scripts.USDA_IWMS import disaggregate_iwms_to_6_digit_naics
from flowsa.data_source_scripts.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup


def load_map_clean_fba(method, attr, fba_sourcename, df_year, flowclass, geoscale_from, geoscale_to, **kwargs):
    """
    Load, clean, and map a FlowByActivity df
    :param method:
    :param attr:
    :param fba_sourcename:
    :param df_year:
    :param flowclass:
    :param geoscale_from:
    :param geoscale_to:
    :param kwargs:
    :return:
    """

    log.info("Loading allocation flowbyactivity " + fba_sourcename + " for year " +
             str(df_year))
    fba = flowsa.getFlowByActivity(datasource=fba_sourcename, year=df_year, flowclass=flowclass)
    fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict)
    fba = harmonize_units(fba)

    # check if allocation data exists at specified geoscale to use
    log.info("Checking if allocation data exists at the " + geoscale_from + " level")
    check_if_data_exists_at_geoscale(fba, geoscale_from)

    # aggregate geographically to the scale of the flowbyactivty source, if necessary
    fba = subset_df_by_geoscale(fba, geoscale_from, geoscale_to)

    # subset based on yaml settings
    if 'flowname_subset' in kwargs:
            if kwargs['flowname_subset'] != 'None':
                fba = fba.loc[fba['FlowName'].isin(kwargs['flowname_subset'])]
    if 'compartment_subset' in kwargs:
        if kwargs['compartment_subset'] != 'None':
            fba = fba.loc[fba['Compartment'].isin(kwargs['compartment_subset'])]

    # cleanup the fba allocation df, if necessary
    if 'clean_fba' in kwargs:
        log.info("Cleaning " + fba_sourcename)
        fba = getattr(sys.modules[__name__], kwargs["clean_fba"])(fba, attr=attr)
    # reset index
    fba = fba.reset_index(drop=True)

    # assign sector to allocation dataset
    log.info("Adding sectors to " + fba_sourcename)
    fba_wsec = add_sectors_to_flowbyactivity(fba, sectorsourcename=method['target_sector_source'])

    # call on fxn to further clean up/disaggregate the fba allocation data, if exists
    if 'clean_fba_w_sec' in kwargs:
        log.info("Further disaggregating sectors in " + fba_sourcename)
        fba_wsec = getattr(sys.modules[__name__], kwargs['clean_fba_w_sec'])(fba_wsec, attr=attr, method=method)

    return fba_wsec
