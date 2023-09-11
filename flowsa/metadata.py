# metadata.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for creating and loading metadata files for
FlowByActivity (FBA) and FlowBySector (FBS) datasets
"""

import pandas as pd
from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file, \
    read_source_metadata
from flowsa.common import return_true_source_catalog_name, get_catalog_info
from flowsa.flowsa_log import log
from flowsa.settings import paths, PKG, PKG_VERSION_NUMBER, WRITE_FORMAT, \
    GIT_HASH, GIT_HASH_LONG


def set_fb_meta(name_data, category):
    """
    Create meta data for a parquet
    :param name_data: string, name of df
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :return: object, metadata for parquet
    """
    fb_meta = FileMeta()
    fb_meta.tool = PKG
    fb_meta.category = category
    fb_meta.name_data = name_data
    fb_meta.tool_version = PKG_VERSION_NUMBER
    fb_meta.git_hash = GIT_HASH
    fb_meta.ext = WRITE_FORMAT
    fb_meta.date_created = \
        pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
    return fb_meta


def write_metadata(source_name, config, fb_meta, category, **kwargs):
    """
    Write the metadata and output as a JSON in a local directory
    :param source_name: string, source name for either a FBA or FBS dataset
    :param config: dictionary, configuration file
    :param fb_meta: object, metadata
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :param kwargs: additional parameters, if running for FBA, define
        "year" of data
    :return: object, metadata that includes methodology for FBAs
    """

    fb_meta.tool_meta = return_fb_meta_data(
        source_name, config, category, **kwargs)
    write_metadata_to_file(paths, fb_meta)


def return_fb_meta_data(source_name, config, category, **kwargs):
    """
    Generate the metadata specific to a Flow-by-Activity or
    Flow-By-Sector method
    :param source_name: string, FBA or FBS method name
    :param config: dictionary, FBA or FBS method
    :param category: string, "FlowByActivity" or "FlowBySector"
    :param kwargs: additional parameters, if running for FBA, define "year"
    :return: object, metadata for FBA or FBS method
    """

    if category == 'FlowBySector':
        method_data = return_fbs_method_data(source_name, config)

    elif category == 'FlowByActivity':
        # when FBA meta created, kwargs exist for year
        year = kwargs['year']
        method_data = return_fba_method_meta(source_name, year=year)
        # return the catalog source name to ensure the method urls are correct
        # for FBAs
        source_name = return_true_source_catalog_name(source_name)

    # create empty dictionary
    fb_dict = {}
    # add url of FlowBy method at time of commit
    fb_dict['method_url'] = \
        f'https://github.com/USEPA/flowsa/blob/{GIT_HASH_LONG}/flowsa/' \
        f'methods/{category.lower()}methods/{source_name}.yaml'

    fb_dict.update(method_data)

    return fb_dict


def return_fbs_method_data(source_name, config):
    """
    Generate the meta data for a FlowBySector dataframe
    :param source_name: string, FBA method name
    :param config: dictionary, configuration/method file
    :return: meta object
    """
    from flowsa.data_source_scripts.stewiFBS import add_stewi_metadata,\
        add_stewicombo_metadata

    # Create empty dictionary for storing meta data
    meta = {}
    # subset the FBS dictionary into a dictionary of source names
    fb = config['source_names']
    # initiate nested dictionary
    meta['primary_source_meta'] = {}
    for k, v in fb.items():
        if k == 'stewiFBS':
            # get stewi metadata
            if v.get('local_inventory_name'):
                meta['primary_source_meta'][k] = add_stewicombo_metadata(
                    v.get('local_inventory_name'))
            else:
                meta['primary_source_meta'][k] = add_stewi_metadata(
                    v['inventory_dict'])
            continue

        cat = (get_catalog_info(k).get('data_format', v.get('data_format', ''))
               .replace('FBS', 'FlowBySector')
               .replace('FBA', 'FlowByActivity'))

        # append source and year
        year = v.get('year', config.get('year'))
        meta['primary_source_meta'][k] = getMetadata(k, year=year,
                                                     category=cat)
        # create dictionary of allocation datasets for different activities
        activities = v.get('activity_sets')
        if activities is None:
            continue
        # initiate nested dictionary
        attr_source_meta = {}
        # subset activity data and allocate to sector
        for aset, attr in activities.items():
            attr_dict = attr.get('attribution_source')
            if attr_dict:
                # append fba meta
                try:
                    source = list(attr_dict.keys())[0]
                    # Check first for cached source
                    if source in config.get('sources_to_cache', ()):
                        year = (config.get('sources_to_cache')
                                      .get(source)
                                      .get('year', v.get('year'))
                                      )
                    else:
                        year = attr_dict[source].get('year', v.get('year'))
                except AttributeError:
                    source = attr_dict
                    year = None
                if source in attr_source_meta.keys():
                    continue
                cat = (get_catalog_info(source)['data_format']
                       .replace('FBS', 'FlowBySector')
                       .replace('FBA', 'FlowByActivity'))

                attr_source_meta[source] = getMetadata(
                    source, year=year, category=cat)

            if 'literature_sources' in attr:
                lit = attr['literature_sources']
                for s, y in lit.items():
                    lit_meta = return_fba_method_meta(s, year=y)
                    # append fba meta
                    attr_source_meta[s] = lit_meta
            if 'attribution_source_meta' in meta['primary_source_meta'][k]:
                meta['primary_source_meta'][k][
                    'attribution_source_meta'].update(attr_source_meta)
            else:
                meta['primary_source_meta'][k][
                    'attribution_source_meta'] = attr_source_meta

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
        fba_dict['data_year'] = kwargs['year']

    try:
        # loop through the FBA yaml and add info
        for k, v in fba.items():
            # include bib_id because this ifno pulled
            # when generating a method bib
            if k in ('author', 'source_name', 'source_url',
                     'original_data_download_date',
                     'date_accessed', 'bib_id'):
                fba_dict[k] = str(v)
    except:
        log.warning('No metadata found for %s', sourcename)
        fba_dict['meta_data'] = f'No metadata found for {sourcename}'

    return fba_dict


def getMetadata(source, year=None, category=None):
    """
    Use the esupy package functions to return the metadata for
    a FBA or FBS used to generate a FBS
    :param source: string, FBA or FBA source name
    :param year: string, year of FBA data, for FBS use None
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :return: meta object, previously generated FBA or FBS meta
    """
    from flowsa.generateflowbyactivity import set_fba_name

    if category is None:
        log.error('Category required, specify "FlowByActivity" or '
                  '"FlowBySector"')
    # if category is FBS ensure year is not added to source name when
    # looking for metadata
    if category == 'FlowBySector':
        year=None

    name = set_fba_name(source, year)
    meta = read_source_metadata(paths, set_fb_meta(name, category))
    if meta is None:
        log.warning('No metadata found for %s', source)
        meta = {'source_meta': f'No metadata found for {name}'}

    return meta
