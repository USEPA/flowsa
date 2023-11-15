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


def import_useeior_mastercrosswalk(year):
    """
    Load USEEIOR's MasterCrosswalk that links BEA data to NAICS
    :return:
    """
    pandas2ri.activate()
    # import the useeior package (r package)
    useeior = packages.importr('useeior')
    # load the .Rd file for
    cw = packages.data(useeior).fetch(
        f'MasterCrosswalk{year}')[f'MasterCrosswalk{year}']

    # save as csv
    cw.to_csv(datapath / f"NAICS_to_BEA_Crosswalk_{year}.csv", index=False)


if __name__ == '__main__':
    import_useeior_mastercrosswalk(year=2017)
