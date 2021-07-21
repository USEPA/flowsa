# metadata.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for creating and loading metadata files for
FlowByActivity (FBA) and FlowBySector (FBS) datasets
"""

import logging as log
import pandas as pd
from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file, read_source_metadata
from flowsa.common import paths, pkg, pkg_version_number, write_format,\
    git_hash, git_hash_long, load_functions_loading_fbas_config, \
    load_fbs_methods_additional_fbas_config


def set_fb_meta(name_data, category):
    """
    Create meta data for a parquet
    :param name_data: string, name of df
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :return: object, metadata for parquet
    """
    fb_meta = FileMeta()
    fb_meta.tool = pkg.project_name
    fb_meta.category = category
    fb_meta.name_data = name_data
    fb_meta.tool_version = pkg_version_number
    fb_meta.git_hash = git_hash
    fb_meta.ext = write_format
    fb_meta.date_created = datetime.now().strftime('%d-%b-%Y')
    return fb_meta


def write_metadata(source_name, config, fb_meta, category, **kwargs):
    """
    Write the metadata and output as a JSON in a local directory
    :param source_name: string, source name for either a FBA or FBS dataset
    :param config: dictionary, configuration file
    :param fb_meta: object, metadata
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :param kwargs: additional parameters, if running for FBA, define "year" of data
    :return: object, metadata that includes methodology for FBAs
    """

    fb_meta.tool_meta = return_fb_meta_data(source_name, config, category, **kwargs)
    write_metadata_to_file(paths, fb_meta)


def return_fb_meta_data(source_name, config, category, **kwargs):
    """
    Generate the metadata specific to a Flow-by-Activity or Flow-By-Sector method
    :param source_name: string, FBA or FBS method name
    :param config: dictionary, FBA or FBS method
    :param fb_meta: object, metadata
    :param category: string, "FlowByActivity" or "FlowBySector"
    :param kwargs: additional parameters, if running for FBA, define "year"
    :return: object, metadata for FBA or FBS method
    """

    # create empty dictionary
    fb_dict = {}

    # add date metadata file generated
    # update the local config with today's date
    fb_dict[f'date_{category}_generated'] = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
    # add url of FlowBy method at time of commit
    fb_dict['method_url'] = f'https://github.com/USEPA/flowsa/blob/{git_hash_long}' \
                 f'/flowsa/data/{category.lower()}methods/{source_name}.yaml'

    if category == 'FlowBySector':
        method_data = return_fbs_method_data(source_name, config)

    elif category == 'FlowByActivity':
        # when FBA meta created, kwargs exist for year
        year = kwargs['year']
        method_data = return_fba_method_meta(source_name, year=year)

    fb_dict.update(method_data)

    return fb_dict


def return_fbs_method_data(source_name, config):
    """
    Generate the meta data for a FlowBySector dataframe
    :param source_name: string, FBA method name
    :param config: dictionary, configuration/method file
    :param fbs_meta: object, FBS metadata to add specific meta to
    :return: meta object
    """

    # load the yaml that lists what additional fbas are used in creating the fbs
    add_fbas = load_fbs_methods_additional_fbas_config()[source_name]

    # Create empty dictionary for storing meta data
    meta = {}
    for x, y in config.items():
        # append k,v if the key contains the phrase "target"
        if 'target' in x:
            meta[x] = y
    # subset the FBS dictionary into a dictionary of source names
    fb = config['source_names']
    for k, v in fb.items():
        # append source and year
        meta[k + '_FBA_meta'] = getMetadata(k, v["year"], paths)
        # create dictionary of allocation datasets for different activities
        activities = v['activity_sets']
        # subset activity data and allocate to sector
        for aset, attr in activities.items():
            # initiate nested dictionary
            meta[k + '_FBA_meta'][aset] = {}
            for aset2, attr2 in attr.items():
                if aset2 in ('allocation_method', 'allocation_source', 'allocation_source_year'):
                    meta[k + '_FBA_meta'][aset][aset2] = str(attr2)
            if attr['allocation_method'] not in (['direct', 'allocation_function']):
                # append fba meta
                meta[k + '_FBA_meta'][aset]['allocation_source_meta'] = \
                    getMetadata(attr['allocation_source'],
                                attr['allocation_source_year'], paths)
            if attr['allocation_helper'] == 'yes':
                for aset2, attr2 in attr.items():
                    if aset2 in ('helper_method', 'helper_source', 'helper_source_year'):
                        meta[k + '_FBA_meta'][aset][aset2] = str(attr2)
                # append fba meta
                meta[k + '_FBA_meta'][aset]['helper_source_meta'] = \
                    getMetadata(attr['helper_source'],
                                attr['helper_source_year'], paths)
            # subset the additional fbas to the source and activity set, if exists
            try:
                fba_sub = add_fbas[k][aset]
                # initiate nested dictionary
                meta[k + '_FBA_meta'][aset]['FBAs_called_within_fxns'] = {}
                for fxn, fba_info in fba_sub.items():
                    # load the yaml with functions loading fbas
                    x = load_functions_loading_fbas_config()[fxn]
                    # initiate nested dictionary
                    meta[k + '_FBA_meta'][aset]['FBAs_called_within_fxns'][fxn] = {}
                    for s, y in fba_info.items():
                        meta[k + '_FBA_meta'][aset]['FBAs_called_within_fxns'][fxn][s] = \
                            getMetadata(x[s]['source'],
                                        y, paths)
            except KeyError:
                pass
            if 'literature_sources' in attr:
                lit = attr['literature_sources']
                # initiate empty dictionary
                meta[k + '_FBA_meta'][aset]['literature_sources_meta'] = {}
                for aset4, attr4 in lit.items():
                    # extract fba meta to append
                    fba_meta = return_fba_method_meta(aset4)
                    # append fba meta
                    meta[k + '_FBA_meta'][aset]['literature_sources_meta'][aset4] = fba_meta
    return meta


def return_fba_method_meta(sourcename, **kwargs):
    """
    Return meta for a FlowByActivity method
    :param sourcename: string, the FlowByActivity sourcename
    :param kwargs: requires "year" defined
    :return: meta object
    """
    from flowsa.bibliography import load_source_dict

    # load info from either a FBA method yaml or the literature yaml
    fba = load_source_dict(sourcename)
    # initiate empty dictionary
    fba_dict = {}

    # add year if creating an FBA metafile
    if 'year' in kwargs:
        fba_dict['fba_data_year'] = kwargs['year']

    try:
        # loop through the FBA yaml and add info
        for k, v in fba.items():
            # include bib_id because this ifno pulled when generating a method bib
            if k in ('fba_author', 'fba_source_name', 'fba_source_url',
                     'original_data_download_date', 'literature_author',
                     'literature_source_name', 'literature_source_url',
                     'date_literature_accessed', 'bib_id'):
                fba_dict[k] = str(v)
    except:
        log.warning(f'No metadata found for {sourcename}')
        fba_dict['meta_data'] = f'No metadata found for {sourcename}'

    return fba_dict


def getMetadata(source, year, paths):
    """
    Use the esupy package functions to return the metadata for
    a FBA used to generate a FBS
    :param source: string, FBA source name
    :param year: string, year of FBA data
    :param paths: paths as defined in common.py
    :return: meta object, previously generated FBA meta
    """
    from flowsa.flowbyactivity import set_fba_name

    name = set_fba_name(source, year)
    try:
        # using 'set_fb_meta' because fxn requires meta object. In the end, the version/git hash are
        # not reset
        meta = read_source_metadata(paths, set_fb_meta(name, 'FlowByActivity'))['tool_meta']
    except:
        meta = {'Warning': f'No metadata found for {source}'}
    return meta
