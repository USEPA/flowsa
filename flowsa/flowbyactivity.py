"""
Helper functions for flowbyactivity data
"""
import numpy as np
from flowsa.common import log, get_county_FIPS, get_state_FIPS, US_FIPS

def filter_by_geographic_scale(flowbyactivity_df, geoscale):
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

def aggregator(flowbyactivity_df, groupbycols):
    """
    Aggregates flowbyactivity_df by given groupbycols
    :param flowbyactivity_df:
    :param groupbycols:
    :return:
    """

    wm = lambda x: np.average(x, weights=flowbyactivity_df.loc[x.index, "FlowAmount"]
    )
    flowbyactivity_df = flowbyactivity_df.groupby(groupbycols, as_index=False).agg({"FlowAmount":"sum",
                                                                    "DataReliability": wm,
                                                                   "DataCollection": wm})
    return flowbyactivity_df


