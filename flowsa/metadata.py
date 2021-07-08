import logging as log

from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file
from flowsa.common import paths, pkg, pkg_version_number, write_format, git_hash, load_sourceconfig


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


def write_metadata(config, fb_meta):
    """
    Save the metadata to a json file
    :return:
    """

    # test
    config = method.copy()
    fb_meta = meta

    # todo: add specific year of FBA created. Perhaps specify the configuration parameters?
    # todo: add time run/meta created
    # todo: append fba-specific info
    method_data = extract_yaml_method_data(config)
    fb_meta.tool_meta = config
    write_metadata_to_file(paths, fb_meta)


def extract_yaml_method_data(config):
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
        meta[k] = v['year']
        dict_key_name = k + '_FBA_meta'
        meta = append_fba_method_meta(meta, k, dict_key_name)
        # fba = load_sourceconfig(k)
        # # initiate nested dictionary
        # meta[k + '_FBA_meta'] = {}
        # for k2, v2 in fba.items():
        #     if k2 in ('author', 'date_downloaded', 'date_generated'):
        #         meta[k + '_FBA_meta'][k2] = str(v2)
            # create dictionary of allocation datasets for different activities
            activities = v['activity_sets']
            # subset activity data and allocate to sector
        for aset, attr in activities.items():
            # print(aset, attr)
            # print(attr['allocation_source'])
            # initiate nested dictionary
            meta[k + '_FBA_meta'][aset] = {}
            for aset2, attr2 in attr.items():
                # print(aset2, attr2)
                if aset2 in ('allocation_method', 'allocation_source', 'allocation_source_year'):
                    meta[k + '_FBA_meta'][aset][aset2] = str(attr2)
                if aset2 == 'allocation_source':
                    if attr2 != 'None':
                        print(attr2)
                        # dict_key_name = [k + '_FBA_meta'][aset]
                        # meta = append_fba_method_meta(meta,, dictionary_key_name)

                # if aset2 == 'allocation_method':
                #     if attr2 not in ('direct', 'allocation_function'):
                #         dict_key_name = [k + '_FBA_meta'][aset]
                #         meta = append_fba_method_meta(meta, , dictionary_key_name)

                        meta[k + '_FBA_meta'][aset][aset2] = attr2


def append_fba_method_meta(dict, sourcename, dictionary_key_name):
    """

    :param dict: dictionary, to which additional FBA information is appended
    :param sourcename: string, the FlowByActivity sourcename
    :return:
    """
    fba = load_sourceconfig(sourcename)
    # initiate nested dictionary
    dict[dictionary_key_name] = {}
    for k, v in fba.items():
        if k in ('author', 'date_downloaded', 'date_generated'):
            dict[sourcename + '_FBA_meta'][k] = str(v)

    return dict
