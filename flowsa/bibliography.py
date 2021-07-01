# bibliography.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Function to generate .bib file for FlowBySector method(s)
"""

import os
import logging as log
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase
from flowsa.flowbysector import load_method
from flowsa.common import outputpath, biboutputpath, load_sourceconfig, \
    load_values_from_literature_citations_config


def generate_list_of_sources_in_fbs_method(methodnames):
    sources = []
    for m in methodnames:
        # load the fbs method yaml
        fbs_yaml = load_method(m)

        # create list of data and allocation data sets
        fbs = fbs_yaml['source_names']
        for fbs_k, fbs_v in fbs.items():
            try:
                sources.append([fbs_k, fbs_v['year']])
            except:
                log.info('Could not append ' + fbs_k + ' to datasource list')
                continue
            activities = fbs_v['activity_sets']
            for aset, attr in activities.items():
                if attr['allocation_source'] != 'None':
                    sources.append([attr['allocation_source'], attr['allocation_source_year']])
                if 'helper_source' in attr:
                    sources.append([attr['helper_source'], attr['helper_source_year']])
                if 'literature_sources' in attr:
                    for source, date in attr['literature_sources'].items():
                        sources.append([source, date])
    return sources

def generate_fbs_bibliography(methodnames):
    """
    Generate bibliography for a FlowBySector
    :param methodname: list of methodnames to create a bibliiography
    :return: a .bib file saved in local directory
    """

    # create list of sources in method
    sources = generate_list_of_sources_in_fbs_method(methodnames)

    # loop through list of sources, load source method yaml, and create bib entry
    bib_list = []
    source_set = set()
    for source in sources:
        # drop list duplicates and any where year is None (because allocation
        # is a function, not a datasource)
        if source[1] != 'None':
            try:
                # first try loading citation information from source yaml
                config = load_sourceconfig(source[0])
            except:
                try:
                    # if no source yaml, check if citation info is for values in the literature
                    config_load = load_values_from_literature_citations_config()
                    config = config_load[source[0]]
                except:
                    log.info('Could not find a method yaml for ' + source[0])
                    continue
            # ensure data sources are not duplicated when different source names
            if (config['source_name_bib'], config['author'], source[1],
                config['citable_url']) not in source_set:
                source_set.add((config['source_name_bib'], config['author'],
                             source[1], config['citable_url']))

                # if there is a date downloaded, use in citation over date generated
                if 'date_downloaded' in config:
                    bib_date = config['date_downloaded']
                else:
                    bib_date = config['date_generated']

                db = BibDatabase()
                db.entries = [{
                    'title': config['source_name_bib'] + ' ' + str(source[1]),
                    'author': config['author'],
                    'year': str(source[1]),
                    'url': config['citable_url'],
                    'urldate': bib_date,
                    'ID': config['bib_id'] + '_' + str(source[1]),
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
