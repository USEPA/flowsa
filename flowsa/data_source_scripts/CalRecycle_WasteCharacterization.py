# CalRecycle_WasteCharacterization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
2014 California Commercial by sector
The original data came from
https://www2.calrecycle.ca.gov/WasteCharacterization/PubExtracts/2014/GenSummary.pdf
The data  was manually scraped so no R/python code is available to replicate.
Last updated:
"""

import os
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.settings import externaldatapath
from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba
from flowsa.sectormapping import get_fba_allocation_subset, add_sectors_to_flowbyactivity



def produced_by(entry):
    """
    Modify source activity names to clarify data meaning
    :param entry: str, original source name
    :return: str, modified activity name
    """
    if "ArtsEntRec" in entry:
        return "Arts Entertainment Recreation"
    if "DurableWholesaleTrucking" in entry:
        return "Durable Wholesale Trucking"
    if "Education" in entry:
        return "Education"
    if "ElectronicEquipment" in entry:
        return "Electronic Equipment"
    if "FoodBeverageStores" in entry:
        return "Food Beverage Stores"
    if "FoodNondurableWholesale" in entry:
        return "Food Nondurable Wholesale"
    if "HotelLodging" in entry:
        return "Hotel Lodging"
    if "MedicalHealth" in entry:
        return "Medical Health"
    if "Multifamily" in entry:
        return "Multifamily"
    if "NotElsewhereClassified" in entry:
        return "Not Elsewhere Classified"
    if "OtherManufacturing" in entry:
        return "Other Manufacturing"
    if "OtherRetailTrade" in entry:
        return "Other Retail Trade"
    if "PublicAdministration" in entry:
        return "Public Administration"
    if "Restaurants" in entry:
        return "Restaurants"
    if "ServicesManagementAdminSupportSocial" in entry:
        return "Services Management Administration Support Social"
    if "ServicesProfessionalTechFinancial" in entry:
        return "Services Professional Technical Financial"
    if "ServicesRepairPersonal" in entry:
        return "Services Repair Personal"


def calR_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    data = {}
    output = pd.DataFrame()
    
    data["Class"] = "Other"
    data['FlowType'] = "WASTE_FLOW"
    data["Location"] = "06000"
    # data["Compartment"] = "ground"
    data["SourceName"] = "CalRecycle_WasteCharacterization"
    data["Year"] = args['year']
    data['DataReliability'] = 5  # tmp
    data['DataCollection'] = 5  # tmp

    for entry in os.listdir(externaldatapath):
        if os.path.isfile(os.path.join(externaldatapath, entry)):
            if "California_Commercial_bySector_2014" in entry and "Map" not in entry:
                data["ActivityProducedBy"] = produced_by(entry)
                dataframe = pd.read_csv(externaldatapath + "/" + entry,
                                        header=0, dtype=str)
                for col in dataframe.columns:
                    if "Percent" in str(col):
                        del dataframe[col]

                for index, row in dataframe.iterrows():
                    data['FlowName'] = row["Material"]
                    for field, value in row[1:].items():
                        col_string = field.split()
                        data["Unit"] = col_string[1].lower()
                        data['Description'] = col_string[0]
                        if value != "-":
                            data["FlowAmount"] = int(value)
                            output = output.append(data, ignore_index=True)
    output = assign_fips_location_system(output, args['year'])
    return output


def keep_generated_quantity(fba, **kwargs):
    """
    Function to clean CalRecycles FBA to remove quantities not assigned as Generated
    :param fba_df: df, FBA format
    :param kwargs: dictionary, can include attr, a dictionary of parameters in 
        the FBA method yaml
    :return: df, modified CalRecycles FBA
    """
    fba = fba[fba['Description']=='Generated']
    return fba
    

def apply_tons_per_employee_per_year_to_states(fbs):
    """
    Calculates tons per employee per year based on BLS_QCEW employees by sector and
    applies that quantity to employees in all states
    """
    bls = load_fba_w_standardized_units(datasource='BLS_QCEW',
                                        year=fbs['Year'].unique()[0],
                                        flowclass='Employment',
                                        geographic_level='state')
    bls = bls[bls['FlowName'] == 'Number of employees']
    # clean df
    bls = clean_bls_qcew_fba(bls)
    bls = add_sectors_to_flowbyactivity(bls)
    
    # Subset BLS dataset
    sector_list = list(filter(None, fbs['SectorProducedBy'].unique()))
    bls = get_fba_allocation_subset(bls, 'BLS_QCEW', sector_list)
    bls = bls.rename(columns={'FlowAmount':'Employees'})
    bls = bls[['Employees','Location','Year','SectorProducedBy']]
    
    # Calculate tons per employee per year per material and sector in CA
    bls_CA = bls[bls['Location']=='06000'] # California
    tpepy = fbs.merge(bls_CA, how = 'inner')
    tpepy['TPEPY'] = np.divide(tpepy['FlowAmount'],tpepy['Employees'],
                               out = np.zeros_like(tpepy['Employees']),
                               where= tpepy['Employees']!=0)
    tpepy = tpepy.drop(columns = ['Employees','FlowAmount','Location'])
    
    # Apply TPEPY back to all employees in all states
    national_waste = tpepy.merge(bls, how = 'outer')
    national_waste['FlowAmount'] = national_waste['Employees'] * national_waste['TPEPY']

    return national_waste

