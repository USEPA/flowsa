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
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.common import externaldatapath


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
        return "Services Professional Technical  Financial"
    if "ServicesRepairPersonal" in entry:
        return "Services Repair Personal"


def calR_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    args = kwargs['args']

    data = {}
    output = pd.DataFrame()

    for entry in os.listdir(externaldatapath):
        if os.path.isfile(os.path.join(externaldatapath, entry)):
            data["Class"] = "Other"
            data['FlowType'] = "Waste Flow"
            data["Location"] = "06000"
            data["Compartment"] = "ground"
            data["LocationSystem"] = "FIPS"
            data["SourceName"] = "California_Commercial_bySector"
            data["Year"] = args['year']
            data['DataReliability'] = 5  # tmp
            data['DataCollection'] = 5  # tmp

            if "California_Commercial_bySector_2014" in entry and "Map" not in entry:
                data["ActivityProducedBy"] = produced_by(entry)
                data["ActivityConsumedBy"] = None
                dataframe = pd.read_csv(externaldatapath + "/" + entry, header=0, dtype=str)
                for col in dataframe.columns:
                    if "Percent" in str(col):
                        del dataframe[col]

                for index, row in dataframe.iterrows():
                    for column in dataframe.columns:
                        if column != "Material":
                            col_string = column.split()
                            data["Unit"] = col_string[1]
                            data['FlowName'] = dataframe.iloc[index]["Material"] + \
                                               " " + col_string[0]
                            if dataframe.iloc[index][column] != "-":
                                data["FlowAmount"] = int(dataframe.iloc[index][column])
                                output = output.append(data, ignore_index=True)
                                output = assign_fips_location_system(output, '2014')
    return output
