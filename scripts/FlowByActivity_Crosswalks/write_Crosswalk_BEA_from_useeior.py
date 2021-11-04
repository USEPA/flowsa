# write_Crosswalk_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
The BEA to NAICS Crosswalk was pulled from USEEIOR's MastereCrosswalk2012.rda on 09/04/2020.

The original file is found here:
https://github.com/USEPA/useeior/blob/master/data/MasterCrosswalk2012.rda

csv obtained by running the following code in Rstudio:
cw  <- load('MasterCrosswalk2012.rda')
write.csv(get(cw), file='NAICS_Crosswalk_BEA.csv')

CSV manually added to flowsa as NAICS_Crosswalk_BEA_raw.csv

"""
