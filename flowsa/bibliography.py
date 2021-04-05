# bibliography.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Function to generate .bib file for FlowBySector method(s)
"""

import os
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase
from flowsa.flowbysector import load_method
from flowsa.common import outputpath, biboutputpath, load_sourceconfig, load_script_fba_citations, \
    load_values_from_literature_citations
import logging as log


def generate_fbs_bibliography(methodnames):
    """
    Generate bibliography for a FlowBySector
    :param methodname: list of methodnames to create a bibliiography
    :return:
    """

    fbas = []
    for m in methodnames:
        # load the fbs method yaml
        fbs_yaml = load_method(m)

        # create list of data and allocation data sets
        fbs = fbs_yaml['source_names']
        for fbs_k, fbs_v in fbs.items():
            try:
                fbas.append([fbs_k, fbs_v['year']])
            except:
                log.info('Could not append ' + fbs_k + ' to datasource list')
                continue
            activities = fbs_v['activity_sets']
            for aset, attr in activities.items():
                if attr['allocation_source'] != 'None':
                    fbas.append([attr['allocation_source'], attr['allocation_source_year']])
                if 'helper_source' in attr:
                    fbas.append([attr['helper_source'], attr['helper_source_year']])
                if 'literature_sources' in attr:
                    for source, date in attr['literature_sources'].items():
                        fbas.append([source, date])

    # loop through list of fbas, load fba method yaml, and create bib entry
    bib_list = []
    fba_set = set()
    for fba in fbas:
        # drop list duplicates and any where year is None (because allocation is a function, not a datasource)
        if fba[1] != 'None':
            try:
                # first try loading citation information from source yaml
                config = load_sourceconfig(fba[0])
            except:
                try:
                    # if no source yaml, check if citation info is for values in the literature
                    config_load = load_values_from_literature_citations()
                    config = config_load[fba[0]]
                except:
                    log.info('Could not find a method yaml for ' + fba[0])
                    continue
            # ensure data sources are not duplicated when different FBA names
            if (config['source_name_bib'], config['author'], fba[1], config['citable_url']) not in fba_set:
                fba_set.add((config['source_name_bib'], config['author'], fba[1], config['citable_url']))

                db = BibDatabase()
                db.entries = [{
                    'title': config['source_name_bib'] + ' ' + str(fba[1]),
                    'author': config['author'],
                    'year': str(fba[1]),
                    'url': config['citable_url'],
                    'urldate': config['date_generated'],
                    'ID': config['bib_id'] + '_' + str(fba[1]),
                    'ENTRYTYPE': 'misc'
                    }]
                # append each entry to a list of BibDatabase entries
                bib_list.append(db)

    # write out bibliography
    writer = BibTexWriter()
    # create directory if missing
    os.makedirs(outputpath + '/Bibliography', exist_ok=True)
    with open(biboutputpath + "_".join(methodnames) + '.bib', 'w') as bibfile:
        # loop through all entries in bib_list
        for b in bib_list:
            bibfile.write(writer.write(b))

    return None
