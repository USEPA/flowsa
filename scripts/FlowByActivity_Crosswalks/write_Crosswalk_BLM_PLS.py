# write_Crosswalk_BLM_PLS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded BLM_PLS to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

"""
import pandas as pd
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""

    df.loc[df['Activity'] == 'Asphalt Competitive Leases', 'Sector'] = '212399' # All Other Nonmetallic Mineral Mining

    df.loc[df['Activity'] == 'Class III Reinstatement Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction

    df.loc[df['Activity'] == 'Coal Licenses, Exploration Licenses', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Coal Licenses, Licenses To Mine', 'Sector'] = '21211'  # Coal Mining

    df.loc[df['Activity'] == 'Combined Hydrocarbon Leases', 'Sector'] = '211112'  # Natural Gas Liquid Extraction
    df.loc[df['Activity'] == 'Competitive General Services Administration (GSA) Oil and Gas Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Competitive National Petroleum Reserve-Alaska Leases, Public Domain', 'Sector'] = '211111'  # Crude Petroleum and Natural Gas Extraction
    df.loc[df['Activity'] == 'Competitive Naval Oil Shale Reserve Leases, Public Domain', 'Sector'] = '211111'  # Crude Petroleum and Natural Gas Extraction

    df.loc[df['Activity'] == 'Competitive Protective Leases, Public Domain and Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Competitive Reform Act Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Competitive Reform Act Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction # todo: added 1/13 - check on results

    df.loc[df['Activity'] == 'EPAct Competitive Geothermal Leases, Public Domain and Acquired Lands', 'Sector'] = '221116'  # Power generation, geothermal

    df.loc[df['Activity'] == 'Exchange Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction

    df.loc[df['Activity'] == 'Federal Coal Leases, Competitive Nonregional Lease-by-Application Leases', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Federal Coal Leases, Competitive Pre-Federal Coal Leasing Amendment Act (FCLAA) Leases', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Federal Coal Leases, Competitive Regional Emergency/Bypass Leases', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Federal Coal Leases, Competitive Regional Leases', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Federal Coal Leases, Exchange Leases', 'Sector'] = '21211'  # Coal Mining
    df.loc[df['Activity'] == 'Federal Coal Leases, Preference Right Leases', 'Sector'] = '21211'  # Coal Mining

    df.loc[df['Activity'] == 'Geothermal Leases, Public Domain and Acquired Lands', 'Sector'] = '221116'  # Power generation, geothermal

    df.loc[df['Activity'] == 'Gilsonite Leases, Gilsonite Competitive Leases', 'Sector'] = '212399'  # Gilsonite mining and/or beneficiating
    df.loc[df['Activity'] == 'Gilsonite Leases, Gilsonite Fringe Acreage Noncompetitive Leases', 'Sector'] = '212399'  # Gilsonite mining and/or beneficiating
    df.loc[df['Activity'] == 'Gilsonite Leases, Gilsonite Preference Right Leases', 'Sector'] = '212399'  # Gilsonite mining and/or beneficiating

    df.loc[df['Activity'] == 'Hardrock - Acquired Lands Leases, Hardrock Preference Right Leases', 'Sector'] = '2122'  # Metal Ore Mining
    df = df.append(pd.DataFrame([['BLM_PLS', 'Hardrock - Acquired Lands Leases, Hardrock Preference Right Leases', '2123']],  # Nonmetallic Mineral Mining and Quarrying
                                columns=['ActivitySourceName', 'Activity', 'Sector']
                                ), ignore_index=True, sort=True)
    # todo: check definition
    df.loc[df['Activity'] == 'Logical Mining Units', 'Sector'] = '21211'  # Coal Mining

    df.loc[df['Activity'] == 'Noncompetitive Pre-Reform Act Future Interest Leases, Public Domain and Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Competitive Pre-Reform Act Future Interest Leases, Public Domain and Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction #todo: added 1/13 - check on output
    df.loc[df['Activity'] == 'Noncompetitive Reform Act Future Interest Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Noncompetitive Reform Act Future Interest Leases, Public Domain and Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Noncompetitive Reform Act Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Noncompetitive Reform Act Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil Shale Leases, Oil Shale R, D&D Leases', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil Shale RD&D Leases', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Pre-Reform Act Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Pre-Reform Act Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Pre-Reform Act Over-the-Counter Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Pre-Reform Act Over-the-Counter Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Special Act - Federal Farm Mortgage Corporation Act of 1934, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Special Act - Rights-of-Way of 1930, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Oil and Gas Special Act - Texas Relinquishment Act of 1919, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction

    df.loc[df['Activity'] == 'Phosphate Leases, Phosphate Competitive Leases', 'Sector'] = '212392' # Phosphate Rock Mining
    df.loc[df['Activity'] == 'Phosphate Leases, Phosphate Fringe Acreage Noncompetitive Leases', 'Sector'] = '212392' # Phosphate Rock Mining
    df.loc[df['Activity'] == 'Phosphate Leases, Phosphate Preference Right Leases', 'Sector'] = '212392' # Phosphate Rock Mining
    df.loc[df['Activity'] == 'Phosphate Use Permits', 'Sector'] = '212392' # Phosphate Rock Mining

    df.loc[df['Activity'] == 'Potassium Leases, Potassium Competitive Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining
    df.loc[df['Activity'] == 'Potassium Leases, Potassium Fringe Acreage Noncompetitive Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining
    df.loc[df['Activity'] == 'Potassium Leases, Potassium Preference Right Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining

    df.loc[df['Activity'] == 'Pre-EPAct Competitive Geothermal Leases, Public Domain and Acquired Lands', 'Sector'] = '221116'  # Power generation, geothermal

    df.loc[df['Activity'] == 'Pre-Reform Act Simultaneous Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Pre-Reform Act Simultaneous Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction #todo: added 1/13 check if needed
    df.loc[df['Activity'] == 'Private Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Reform Act Leases, Acquired Lands', 'Sector'] = '21111'  # Oil and Gas Extraction
    df.loc[df['Activity'] == 'Renewal Leases, Public Domain', 'Sector'] = '21111'  # Oil and Gas Extraction

    df.loc[df['Activity'] == 'Sodium Leases, Sodium Competitive Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining
    df.loc[df['Activity'] == 'Sodium Leases, Sodium Fringe Acreage Noncompetitive Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining
    df.loc[df['Activity'] == 'Sodium Leases, Sodium Preference Right Leases', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining
    df.loc[df['Activity'] == 'Sodium Use Permit', 'Sector'] = '212391' # Potash, Soda, and Borate Mineral Mining

    # todo: look over data - check next activity is double counting
    #df.loc[df['Activity'] == 'Summary: Pre-Reform Act Simultaneous Leases, Public Domain and Acquired Lands', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2007', '2011', '2012']
    # assign flowclass
    flowcass = ['Land']
    # assign datasource
    datasource = 'BLM_PLS'
    # df of unique ers activity names
    df = unique_activity_names(flowcass, years, datasource)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'
    # drop any rows where naics12 is 'nan' (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = "I"
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
