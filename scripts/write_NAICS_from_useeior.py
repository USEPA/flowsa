# write_NAICS_from_useeior.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
A script to get NAICS names and a NAICS 2-3-4-5-6 crosswalk.

- from useeior amd store them as .csv.
- Depends on rpy2 and tzlocal as well as having R installed and useeior installed.

"""

import rpy2.robjects.packages as packages
from rpy2.robjects import pandas2ri
from flowsa.settings import datapath


def import_useeior_mastercrosswalk():
    """
    Load USEEIOR's MasterCrosswalk that links BEA data to NAICS
    :return:
    """
    pandas2ri.activate()
    # import the useeior package (r package)
    useeior = packages.importr('useeior')
    # load the .Rd file for
    cw = packages.data(useeior).fetch(
        'MasterCrosswalk2012')['MasterCrosswalk2012']

    # save as csv
    cw.to_csv(datapath + "NAICS_to_BEA_Crosswalk.csv", index=False)


if __name__ == '__main__':
    import_useeior_mastercrosswalk()
