"""
Helper functions for flowbyactivity data
"""
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS, activity_fields


fba_activity_fields = [activity_fields['ProducedBy'][0]['flowbyactivity'],
                       activity_fields['ConsumedBy'][0]['flowbyactivity']]

fba_default_grouping_fields = ['FlowName','Unit',
                               'ActivityProducedBy',
                               'ActivityConsumedBy',
                               'Compartment', 'FIPS', 'Year']

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
    #breaking here
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
    flowbyactivity_dfg = flowbyactivity_df.groupby(groupbycols, as_index=False).agg({"FlowAmount":"sum"})

    #flowbyactivity_dfg = flowbyactivity_df.groupby(groupbycols).agg({"FlowAmount":"sum",
    #                                                                "DataReliability": wm,
    #                                                               "DataCollection": wm})
    return flowbyactivity_dfg


