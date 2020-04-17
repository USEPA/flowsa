"""
Helper functions for flowbyactivity data
"""
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields, \
    flow_by_activity_fields


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

def create_fill_na_dict():
    fill_na_dict = {}
    for k,v in flow_by_activity_fields.items():
        if v[0]['dtype']=='str':
            fill_na_dict[k] = ""
        elif v[0]['dtype']=='int':
            fill_na_dict[k] = 9999
        elif v[0]['dtype']=='float':
            fill_na_dict[k] = 0.0
    return fill_na_dict

fba_fill_na_dict =  create_fill_na_dict()

def get_fba_groupby_cols():
    groupby_cols = []
    for k,v in flow_by_activity_fields.items():
        if v[0]['dtype']=='str':
            groupby_cols.append(k)
        elif v[0]['dtype']=='int':
            groupby_cols.append(k)
    #Do not use description for grouping
    groupby_cols.remove('Description')
    return groupby_cols

fba_default_grouping_fields = get_fba_groupby_cols()

def filter_by_geoscale(flowbyactivity_df, geoscale):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param flowbyactitvy_df:
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity
    """
    fips = []
    #all_FIPS = read_stored_FIPS()
    if (geoscale == "national"):
        fips.append(US_FIPS)
    elif (geoscale == "state"):
        state_FIPS = get_state_FIPS()
        fips = list(state_FIPS['FIPS'])
    elif (geoscale == "county"):
        county_FIPS = get_county_FIPS()
        fips = list(county_FIPS['FIPS'])

    flowbyactivity_df = flowbyactivity_df[flowbyactivity_df['FIPS'].isin(fips)]
    if len(flowbyactivity_df) == 0:
        log.error("No flows found in the flow dataset at the " + geoscale + " scale.")
    else:
        return flowbyactivity_df

def agg_by_geoscale(flowbyactivity_df, from_scale, to_scale):
    """

    :param flowbyactivity_df:
    :param from_scale:
    :param to_scale:
    :return:
    """
    from flowsa.common import fips_number_key
    from_scale_dig = fips_number_key[from_scale]
    to_scale_dig = fips_number_key[to_scale]

    #use from scale to filter by these values
    fba_from_scale = filter_by_geoscale(flowbyactivity_df,from_scale)

    group_cols = fba_default_grouping_fields.copy()
    group_cols.remove('FIPS')
    if to_scale == 'state':
        fba_from_scale['to_FIPS'] = fba_from_scale['FIPS'].apply(lambda x: str(x[0:2]))
        group_cols.append('to_FIPS')
    #if national no need to do anything
    fba_agg = aggregator(fba_from_scale,group_cols)
    return fba_agg




def aggregator(flowbyactivity_df, groupbycols):
    """
    Aggregates flowbyactivity_df by given groupbycols
    :param flowbyactivity_df:
    :param groupbycols:
    :return:
    """
    wm = lambda x: np.average(x, weights=flowbyactivity_df.loc[x.index, "FlowAmount"])
    agg_funx = {"FlowAmount":"sum",
                "Spread":wm,
                "DataReliability": wm,
                "DataCollection": wm}
    flowbyactivity_dfg = flowbyactivity_df.groupby(groupbycols, as_index=False).agg(agg_funx)
    return flowbyactivity_dfg


def add_missing_flow_by_activity_fields(flowbyactivity_partial_df):
    """
    Add in missing fields to have a complete and ordered
    :param flowbyactivity_partial_df:
    :return:
    """
    for k in flow_by_activity_fields.keys():
        if k not in flowbyactivity_partial_df.columns:
            flowbyactivity_partial_df[k] = None
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flow_by_activity_fields.items():
        flowbyactivity_partial_df[k] = flowbyactivity_partial_df[k].astype(v[0]['dtype'])
    # Resort it so order is correct
    flowbyactivity_partial_df = flowbyactivity_partial_df[flow_by_activity_fields.keys()]
    return flowbyactivity_partial_df

def check_fba_fields(flowbyactivity_df):
    """
    Add in missing fields to have a complete and ordered
    :param flowbyactivity_partial_df:
    :return:
    """
    for k,v in flow_by_activity_fields.items():
        try:
            log.debug("fba activity " + k + " data type is " + str(flowbyactivity_df[k].values.dtype))
            log.debug("standard " + k + " data type is " + str(v[0]['dtype']))
        except:
            log.debug("Failed to find field ",k," in fba")

