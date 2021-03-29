# __init__.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Writes a .bib for sources in a FlowBySector method yaml
"""

import flowsa

# write bib file to local directory, FBS methods must be in list
flowsa.writeFlowBySectorBibliography(['Land_national_2012', 'Water_national_2015_m1'])
