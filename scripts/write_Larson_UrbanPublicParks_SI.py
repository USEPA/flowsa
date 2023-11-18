# write_Larson_UrbanPublicParks_SI.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Load and save the SI parks data from

    Larson LR, Jennings V, Cloutier SA (2016) Public Parks and
    Wellbeing in Urban Areas of the United States.
    PLoS ONE 11(4): e0153211. https://doi.org/10.1371/journal.pone.0153211

SI obtained 08/26/2020
"""

import io
import pandas as pd
from esupy.remote import make_url_request
from flowsa.settings import externaldatapath

# 2012--2018 fisheries data at state level
csv_load = "https://doi.org/10.1371/journal.pone.0153211.s001"


if __name__ == '__main__':

    response = make_url_request(csv_load)
    # Read directly into a pandas df
    raw_df = pd.read_excel(io.BytesIO(response.content)).dropna().reset_index(drop=True)
    # save data to csv
    raw_df.to_csv(f"{externaldatapath}/Larson_UrbanPublicParks_SI.csv",
                  index=False)
