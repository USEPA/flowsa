# ca_waste.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
2014 California Commercial by sector
The original data came from
https://www2.calrecycle.ca.gov/WasteCharacterization/PubExtracts/2014/GenSummary.pdf
The data  was manually scraped so no R/python code is available to replicate.
Last updated:
"""

import pandas as pd
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields, assign_fips_location_system, clean_df, fba_fill_na_dict
from flowsa.common import *
import os



def produced_by(entry):
    if "ArtsEntRec" in entry:
        return "Arts Entertainment Recreation"
    elif "DurableWholesaleTrucking" in entry:
        return "Durable Wholesale Trucking"
    elif "Education" in entry:
        return "Education"
    elif "ElectronicEquipment" in entry:
        return "Electronic Equipment"
    elif "FoodBeverageStores" in entry:
        return "Food Beverage Stores"
    elif "FoodNondurableWholesale" in entry:
        return "Food Nondurable Wholesale"
    elif "HotelLodging" in entry:
        return "Hotel Lodging"
    elif "MedicalHealth" in entry:
        return "Medical Health"
    elif "Multifamily" in entry:
        return "Multifamily"
    elif "NotElsewhereClassified" in entry:
        return "Not Elsewhere Classified"
    elif "OtherManufacturing" in entry:
        return "Other Manufacturing"
    elif "OtherRetailTrade" in entry:
        return "Other Retail Trade"
    elif "PublicAdministration" in entry:
        return "Public Administration"
    elif "Restaurants" in entry:
        return "Restaurants"
    elif "ServicesManagementAdminSupportSocial" in entry:
        return "Services Management Administration Support Social"
    elif "ServicesProfessionalTechFinancial" in entry:
        return "Services Professional Technical  Financial"
    elif "ServicesRepairPersonal" in entry:
        return "Services Repair Personal"
    else:
        return ""

external_datapath = datapath + "external_data"

if __name__ == '__main__':
    df_list = []
    data = {}
    output = pd.DataFrame()

    for entry in os.listdir(external_datapath):
        if os.path.isfile(os.path.join(external_datapath, entry)):
            data["Class"] = "Other"
            data['FlowType'] = "Waste Flow"
            data["Location"] = "06000"
            data["Compartment"] = "ground"
            data["LocationSystem"] = "FIPS"
            data["SourceName"] = "California_Commercial_bySector"
            data["Year"] = 2014

            if "California_Commercial_bySector_2014" in entry and "Map" not in entry:
                data["ActivityProducedBy"] = produced_by(entry)
                data["ActivityConsumedBy"] = None
                dataframe = pd.read_csv(external_datapath + "/" + entry, header=0, dtype=str)
                for col in dataframe.columns:
                    if "Percent" in str(col):
                        del dataframe[col]

                for index, row in dataframe.iterrows():
                    for column in dataframe.columns:
                        if "Material" != column:
                            col_string = column.split()
                            data["Unit"] = col_string[1]
                            data['FlowName'] = dataframe.iloc[index]["Material"] + " " + col_string[0]
                            if dataframe.iloc[index][column] != "-":
                                data["FlowAmount"] = int(dataframe.iloc[index][column])
                                output = output.append(data, ignore_index=True)
                                output = assign_fips_location_system(output, '2014')
    flow_df = add_missing_flow_by_fields(output, flow_by_activity_fields)
    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = clean_df(flow_df, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)
    parquet_name = 'California_Commercial_bySector'
    store_flowbyactivity(flow_df, parquet_name, 2014)



