# metadata.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for creating and loading metadata files for FlowByActivity (FBA) and FlowBySector (FBS) datasets
"""

import logging as log
import pandas as pd
import os
import re
import json
from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file, load_preprocessed_output
from esupy.util import strip_file_extension
from flowsa.common import paths, pkg, pkg_version_number, write_format,\
    git_hash, git_hash_long, default_download_if_missing, fbaoutputpath


def set_fb_meta(name_data, category):
    """
    Create meta data for a parquet
    :param name_data: name of df
    :param category: 'FlowBySector' or 'FlowByActivity'
    :return: metadata for parquet
    """
    fb_meta = FileMeta()
    fb_meta.name_data = name_data
    fb_meta.tool = pkg.project_name
    fb_meta.tool_version = pkg_version_number
    fb_meta.category = category
    fb_meta.ext = write_format
    fb_meta.git_hash = git_hash
    return fb_meta


def write_metadata(source_name, config, fb_meta, category, **kwargs):
    """
    Save the metadata to a json file
    :param category: 'FlowBySector' or 'FlowByActivity'
    :return:
    """

    fb_meta.tool_meta = return_fb_meta_data(source_name, config, category, **kwargs)
    write_metadata_to_file(paths, fb_meta)


def return_fb_meta_data(source_name, config, category, **kwargs):
    """

    :param source_name:
    :param config:
    :param category:
    :return:
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
        method_data = return_fbs_method_data(config)

    elif category == 'FlowByActivity':
        # when FBA meta created, kwargs exist for year
        year = kwargs['year']
        method_data = return_fba_method_meta(source_name, year)

    fb_dict.update(method_data)

    return fb_dict


def return_fbs_method_data(config):
    """

    :param config: dictionary, FBS method yaml
    :return:
    """

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
        meta['datasource'] = k
        meta[k + '_FBA_meta'] = getMetadata(k, v["year"], paths)["tool_meta"]
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
                                attr['allocation_source_year'], paths)["tool_meta"]
            if attr['allocation_helper'] == 'yes':
                for aset2, attr2 in attr.items():
                    if aset2 in ('helper_method', 'helper_source', 'helper_source_year'):
                        meta[k + '_FBA_meta'][aset][aset2] = str(attr2)
                # append fba meta
                meta[k + '_FBA_meta'][aset]['helper_source_meta'] = \
                    getMetadata(attr['helper_source'],
                                attr['helper_source_year'], paths)["tool_meta"]
            # if 'fbas_called_within_fxns' in attr:
            #     fbas = attr['fbas_called_within_fxns']
            #     # initiate empty dictionary
            #     meta[k + '_FBA_meta'][aset]['fbas_called_within_fxns'] = {}
            #     for aset3, attr3 in fbas.items():
            #         # extract fba meta to append
            #         fba_meta = return_fba_method_meta(attr3['source'])
            #         # append fba meta
            #         meta[k + '_FBA_meta'][aset]['fbas_called_within_fxns'][attr3['source']] = \
            #         getMetadata(attr['helper_source'],
            #                     attr['helper_source_year'], paths)["tool_meta"]
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


def return_fba_method_meta(sourcename, year):
    """

    :param sourcename: string, the FlowByActivity sourcename
    :return:
    """
    from flowsa.bibliography import load_source_dict

    # load info from either a FBA method yaml or the literature yaml
    fba = load_source_dict(sourcename)
    # initiate empty dictionary
    fba_dict = {}

    # add year
    fba_dict['fba_data_year'] = year

    # loop through the FBA yaml and add info
    for k, v in fba.items():
        if k in ('fba_author', 'fba_source_name', 'fba_source_url',
                 'date_literature_accessed', 'original_data_download_date'):
            fba_dict[k] = str(v)

    return fba_dict


def return_fba_metadata_file_path(file_name, paths):
    """
    Searches for file within path.local_path based on file metadata, if metadata matches,
     returns most recently created file name
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :param force_version: boolean on whether or not to include version number in search
    :return: str with the file path if found, otherwise an empty string
    """
    path = os.path.realpath(paths.local_path + "/FlowByActivity")
    if os.path.exists(path):
        search_words = file_name
        matches = []
        fs = {}
        for f in os.scandir(path):
            name = f.name
            # get file creation time
            st = f.stat().st_ctime
            fs[name]=st
            matches = []
            for k in fs.keys():
                if re.search(search_words, k):
                    if re.search(".parquet", k, re.IGNORECASE):
                        matches.append(k)
        if len(matches) == 0:
            f = ""
        else:
            # Filter the dict by matches
            r = {k:v for k,v in fs.items() if k in matches}
            # Sort the dict by matches, return a list
            #r = {k:v for k,v in sorted(r.items(), key=lambda item: item[1], reverse=True)}
            rl = [k for k,v in sorted(r.items(), key=lambda item: item[1], reverse=True)]
            f = os.path.realpath(path + "/" + rl[0])
    else:
        f = ""

    # strip file extension and return file name of parquet
    f = strip_file_extension(f)
    f = f'{f}_metadata.json'

    return f


def read_source_metadata(filepath):
    """return the locally saved metadata dictionary from JSON
    :param filepath: str in the form of dir/inv_year
    :return: metadata dictionary
    """

    try:
        with open(filepath, 'r') as file:
            file_contents = file.read()
            metadata = json.loads(file_contents)
            return metadata
    except FileNotFoundError:
        log.warning("metadata not found for source data")
        return None


def getMetadata(source, year, paths):
    from flowsa.flowbyactivity import set_fba_name

    name = set_fba_name(source, year)
    fba_file_path = return_fba_metadata_file_path(name, paths)
    meta = read_source_metadata(fba_file_path)
    return meta
