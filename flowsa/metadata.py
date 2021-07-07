import logging as log

from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file
from flowsa.common import paths, pkg, pkg_version_number, write_format, git_hash


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
    # todo: add specific year of FBA created. Perhaps specify the configuration parameters?
    # todo: add time run/meta created
    # todo: append fba-specific info
    fb_meta.tool_meta = config
    write_metadata_to_file(paths, fb_meta)


# def loop_fbs():
#     """
#
#     :return:
#     """
#     method_name = kwargs['method']
#     # assign arguments
#     log.info("Initiating flowbysector creation for " + method_name)
#     # call on method
#     method = load_method(method_name)
#     # create dictionary of data and allocation datasets
#     fb = method['source_names']
#     # Create empty list for storing fbs files
#     fbs_list = []
#     for k, v in fb.items():
#         # pull fba data for allocation
#         flows = load_source_dataframe(k, v)
#
#         if v['data_format'] == 'FBA':
#             # ensure correct datatypes and that all fields exist
#             flows = clean_df(flows, flow_by_activity_fields,
#                              fba_fill_na_dict, drop_description=False)
#
#             # clean up fba, if specified in yaml
#             if v["clean_fba_df_fxn"] != 'None':
#                 log.info("Cleaning up " + k + " FlowByActivity")
#                 flows = dynamically_import_fxn(k, v["clean_fba_df_fxn"])(flows)
#
#             # if activity_sets are specified in a file, call them here
#             if 'activity_set_file' in v:
#                 aset_names = pd.read_csv(flowbysectoractivitysetspath +
#                                          v['activity_set_file'], dtype=str)
#             else:
#                 aset_names = None
#
#             # create dictionary of allocation datasets for different activities
#             activities = v['activity_sets']
#             # subset activity data and allocate to sector
#             for aset, attr in activities.items():
