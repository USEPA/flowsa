# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class
"""
import flowsa
import yaml
from flowsa.common import log, flowbyactivitymethodpath
from flowsa.mapping import add_sectors_to_flowbyactivity
from flowsa.flowbyactivity import filter_by_geoscale, fba_activity_fields, agg_by_geoscale,create_fill_na_dict


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name:
    :return:
    """
    sfile = flowbyactivitymethodpath+method_name+'.yaml'
    try:
        with open(sfile, 'r') as f:
            method = yaml.safe_load(f)
    except IOError:
        log.error("File not found.")
    return method




#Need to create sums by flowtype and unit to check against later

#Add in Sector matches now to producing and consuming activities
#df = add_sectors_to_flowbyactivity(df)
#Very important to note that values have not been allocated but are repeated at this point


# Removing records that we don't need

def main(method_name):
    """
    Creates a flowbysector dataset
    :param method_name: Name of method corresponding to flowbysector method yaml name
    :return: flowbysector
    """

    method = load_method(method_name)
    fbas = method['flowbyactivity_sources']
    for k,v in fbas.items():
        print(k)
        flows = flowsa.getFlowByActivity(flowclass=v['class'],
                                                    years=[v['year']],
                                                    datasource = k)
        #drop description field
        flows = flows.drop(columns='Description')
        #fill null values
        fill_dict = create_fill_na_dict()
        flows = flows.fillna(value=fill_dict)
        activities = v['activity_sets']
        for aset,attr in activities.items():
            # subset by named activities
            names = [attr['names']]
            flows = flows[(flows[fba_activity_fields[0]].isin(names)) |
                          (flows[fba_activity_fields[1]].isin(names))]
            #Reset index values after subset
            flows = flows.reset_index()
            #aggregate geographically
            from_scale = v['geoscale_to_use']
            to_scale = method['target_geoscale']
            flows = agg_by_geoscale(flows, from_scale, to_scale)


        flows = add_sectors_to_flowbyactivity(flows,sectorsourcename=method['target_sector_source'])

    return flows

















