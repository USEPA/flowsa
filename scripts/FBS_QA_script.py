# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 09:53:24 2020

@author: BYoung
"""

import flowsa
import pandas as pd
from flowsa.common import fbaoutputpath, fbsoutputpath, datapath, log, \
    flowbysectoractivitysetspath
from flowsa.flowbyfunctions import agg_by_geoscale, fba_default_grouping_fields
from flowsa.mapping import map_elementary_flows


fba_source = 'EPA_NEI_Nonpoint'
asets_source = 'NEI_Nonpoint_2017_asets.csv'
fba_agg = 'county'
fba_class = 'Chemicals'
fba_year = 2017
fbs_source = 'Air_NEI_national_2017'
subset_activities = False

def get_fba_subset(name, year, flowclass):
    test_fba = flowsa.getFlowByActivity(flowclass=[flowclass], years=[year],datasource=name)
    
    if subset_activities:
        aset_names = pd.read_csv(flowbysectoractivitysetspath+asets_source,dtype=str)
        asets = [
            #'activity_set_1', 
            #'activity_set_2', 
            #'activity_set_3',
            #'activity_set_4', 
            'activity_set_5', 
            'activity_set_6', 
            ]
        activities = aset_names[aset_names['activity_set'].isin(asets)]['name']
        test_fba = test_fba[test_fba['ActivityProducedBy'].isin(activities)]
    return test_fba

def get_fbs_subset(name):
    test_fbs = flowsa.getFlowBySector(name)
    return test_fbs
    

if __name__ == '__main__':
    fba = get_fba_subset(fba_source, fba_year, fba_class)
    fba = agg_by_geoscale(fba, fba_agg,'national', fba_default_grouping_fields)
    
    fba = fba[['FlowName','ActivityProducedBy','FlowAmount','Unit','Compartment']]
    fba = map_elementary_flows(fba, 'NEI')
    fba_pivot = pd.pivot_table(fba, values = 'FlowAmount', index =['Flowable'], columns='ActivityProducedBy', aggfunc='sum', margins = True).reset_index()
    
    fbs = get_fbs_subset(fbs_source)
    fbs = fbs[['Flowable','SectorProducedBy','FlowAmount']]
    fbs_pivot = pd.pivot_table(fbs, values = 'FlowAmount', index =['Flowable'], columns='SectorProducedBy', aggfunc='sum', margins = True).reset_index()
    
    fba = fba.groupby('Flowable').agg({'FlowAmount': 'sum'})
    fba.rename(columns={'FlowAmount':'FBA_amount'}, inplace=True)
    fbs = fbs.groupby('Flowable').agg({'FlowAmount': 'sum'})
    fbs.rename(columns={'FlowAmount':'FBS_amount'}, inplace=True)

    comparison = fba.merge(fbs, how='outer', on ='Flowable')
    comparison['Ratio'] = comparison['FBS_amount'] / comparison ['FBA_amount']    
    
    