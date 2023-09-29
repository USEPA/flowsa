# bibliography.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to generate .bib file for a FlowBySector method
"""
# todo: modify the functions for recursive yaml method

import pandas as pd
from bibtexparser.bwriter import BibTexWriter
from bibtexparser.bibdatabase import BibDatabase
from esupy.processed_data_mgmt import mkdir_if_missing

from flowsa.common import load_values_from_literature_citations_config, \
    get_flowsa_base_name, sourceconfigpath, load_yaml_dict
from flowsa.flowsa_log import log
from flowsa.settings import outputpath, biboutputpath


def generate_list_of_sources_in_fbs_method(methodname):
    """
    Determine what FlowByActivities are used to generate a FlowBySector
    :param methodname: string, FlowBySector method
    :return: list, pairs of FlowByActivity source names and years
    """
    sources = []
    # load the fbs method yaml
    fbs_yaml = load_yaml_dict(methodname, flowbytype='FBS')

    # create list of data and allocation data sets
    fbs = fbs_yaml['source_names']
    for fbs_k, fbs_v in fbs.items():
        try:
            sources.append([fbs_k, fbs_v['year']])
        except KeyError:
            log.info(f'Could not append {fbs_k} to datasource '
                     'list because missing year')
            continue
        activities = fbs_v['activity_sets']
        for aset, attr in activities.items():
            if attr['allocation_source'] != 'None':
                sources.append([attr['allocation_source'],
                                attr['allocation_source_year']])
            if 'helper_source' in attr:
                sources.append([attr['helper_source'],
                                attr['helper_source_year']])
            if 'literature_sources' in attr:
                for source, date in attr['literature_sources'].items():
                    sources.append([source, date])

    return sources


def load_source_dict(sourcename):
    """
    Load the yaml method file for a flowbyactivity dataset
    or for a value from the literature
    :param sourcename: string, FBA source name or value from the lit name
    :return: dictionary, the method file
    """

    try:
        # check if citation info is for values in the literature
        config_load = load_values_from_literature_citations_config()
        config = config_load[sourcename]
    except KeyError:
        # else check if file exists, then try loading
        # citation information from source yaml
        sourcename = get_flowsa_base_name(sourceconfigpath, sourcename, "yaml")
        config = load_yaml_dict(sourcename, flowbytype='FBA')

    return config


# def generate_fbs_bibliography(methodname):
#     """
#     Generate bibliography for a FlowBySector
#     :param methodname: string, methodname to create a bibliiography
#     :return: a .bib file saved in local directory
#     """
#
#     from flowsa.metadata import getMetadata
#
#     # create list of sources in method
#     sources = generate_list_of_sources_in_fbs_method(methodname)
#
#     # loop through list of sources, load source method
#     # yaml, and create bib entry
#     bib_list = []
#     source_set = set()
#     for source in sources:
#         # drop list duplicates and any where year is None (because allocation
#         # is a function, not a datasource)
#         if source[1] != 'None':
#             try:
#                 config = \
#                     load_values_from_literature_citations_config()[source[0]]
#             except KeyError:
#                 try:
#                     config = getMetadata(source[0], source[1])
#                     # flatten the dictionary so can treat all
#                     # dictionaries the same when pulling info
#                     config = pd.json_normalize(config, sep='_')
#                     config.columns = \
#                         config.columns.str.replace('tool_meta_', '')
#                     config = config.to_dict(orient='records')[0]
#                 except KeyError or AttributeError:
#                     log.info('Could not find metadata for %s', source[0])
#                     continue
#             if config is not None:
#                 # ensure data sources are not duplicated
#                 # when different source names
#                 try:
#                     if (config['source_name'], config['author'], source[1],
#                             config['source_url']) not in source_set:
#                         source_set.add((config['source_name'],
#                                         config['author'],
#                                         source[1],
#                                         config['source_url']))
#
#                         # if there is a date downloaded, use in
#                         # citation over date generated
#                         if 'original_data_download_date' in config:
#                             bib_date = config['original_data_download_date']
#                         elif 'date_accessed' in config:
#                             bib_date = config['date_accessed']
#                         else:
#                             bib_date = config['date_created']
#
#                         db = BibDatabase()
#                         db.entries = [{
#                             'title': f"{config['source_name']} "
#                                      f"{str(source[1])}",
#                             'author': config['author'],
#                             'year': str(source[1]),
#                             'url': config['tool_meta']['source_url'],
#                             'urldate': bib_date,
#                             'ID': config['tool_meta']['bib_id'] + '_' + str(source[1]),
#                             'ENTRYTYPE': 'misc'
#                         }]
#                         # append each entry to a list of BibDatabase entries
#                         bib_list.append(db)
#                 except KeyError:
#                     log.exception('Missing information needed to '
#                                   'create bib for %s, %s', source[0],
#                                   source[1])
#                     continue
#
#     # write out bibliography
#     writer = BibTexWriter()
#     # create directory if missing
#     mkdir_if_missing(outputpath / 'Bibliography')
#     with open(biboutputpath / f'{methodname}.bib', 'w') as bibfile:
#         # loop through all entries in bib_list
#         for b in bib_list:
#             bibfile.write(writer.write(b))


# todo: will reintroduce option to create bibliography post 2.0 release
# def writeFlowBySectorBibliography(methodname):
#     """
#     Generate bibliography for FlowBySectorMethod in local directory
#     :param methodname: string, FBS methodname for which to create .bib file
#     :return: .bib file save to local directory
#     """
#     # Generate a single .bib file for a list of Flow-By-Sector method names
#     # and save file to local directory
#     log.info(f'Write bibliography to {biboutputpath / methodname}.bib')
#     generate_fbs_bibliography(methodname)
