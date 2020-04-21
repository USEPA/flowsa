# write_NAICS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Grabs NAICS 2007, 2012, and 2017 codes from useeior.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri


# does not work due to issues with rpy2. Crosswalk was manually copied from useeior and added as csv (4/18/2020)
pandas2ri.activate()

useeior = importr('useeior')

NAICS_crosswalk = useeior.getMasterCrosswalk(2012)
NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)
NAICS_crosswalk.to_csv(datapath+"NAICS_07_to_17_Crosswalk.csv", index=False)



