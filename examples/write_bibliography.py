# write_bibliography.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Writes a .bib for sources in a FlowBySector method yaml
"""

import flowsa


def main():
    # write .bib file to local directory for a Flow-By-Sector method
    flowsa.writeFlowBySectorBibliography('Land_national_2012')


if __name__ == "__main__":
    main()
