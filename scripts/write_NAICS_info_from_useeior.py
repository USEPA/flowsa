# write_NAICS_info_from_useeior.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
A script to get NAICS names and a NAICS 2-3-4-5-6 crosswalk.

- from useeior amd store them as .csv.
- Depends on rpy2 and tzlocal as well as having R installed and useeior installed.
"""

from flowsa.common import datapath
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
pandas2ri.activate()

useeior = importr('useeior')

NAICS_crosswalk = useeior.getNAICS2to6Digits(2012)
NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)
NAICS_crosswalk.to_csv(datapath+"NAICS_2012_2to6_Crosswalk.csv", index=False)

NAICS_names = useeior.getNAICSCodeName(2012)
NAICS_names = pandas2ri.ri2py_dataframe(NAICS_names)
NAICS_names.to_csv(datapath+"NAICS_2012_Names.csv", index=False)
