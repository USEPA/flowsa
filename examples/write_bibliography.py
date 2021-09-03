# write_bibliography.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Writes a .bib for sources in a FlowBySector method yaml

If you encounter the error:
pkg_resources.DistributionNotFound: The 'flowsa' distribution was not
found and is required by the application

See the flowsa wiki:
https://github.com/USEPA/flowsa/wiki/Using-FLOWSA-as-a-Developer#troubleshooting
"""

import flowsa

# write .bib file to local directory for a Flow-By-Sector method
flowsa.writeFlowBySectorBibliography('Land_national_2012')
