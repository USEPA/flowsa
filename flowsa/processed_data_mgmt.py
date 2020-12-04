"""
Functions to manage querying, retrieving and storing processed flowbyactivity and flowbysector data
in both local directories and a remote data server
"""
import datetime as dt
import json
import logging as log
import os
import appdirs
import pandas as pd

from flowsa.common import make_http_request, strip_file_extension

fba_remote_path = "https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowByActivity/"
fbs_remote_path = "https://edap-ord-data-commons.s3.amazonaws.com/flowsa/FlowBySector/"
local_storage_path = os.path.realpath(appdirs.user_data_dir()+"/flowsa")
fba_local_path = os.path.realpath(local_storage_path + "/FlowByActivity/")
fbs_local_path = os.path.realpath(local_storage_path + "/FlowBySector/")


def get_file_update_time_from_DataCommons(datafile):
    """
    Gets a datetime object for the file on the DataCommons server
    :param datafile:
    :return:
    """
    base_url = "https://xri9ebky5b.execute-api.us-east-1.amazonaws.com/api/?"
    search_param = "searchvalue"
    url = base_url + search_param + "=" + datafile + "&place=&searchfields=filename"
    r = make_http_request(url)
    date_str = r.json()[0]["LastModified"]
    file_upload_dt = dt.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S%z')
    return file_upload_dt


def get_file_update_time_from_local(datafile,path):
    meta = read_datafile_meta(datafile,path)
    file_upload_dt = dt.datetime.strptime(meta["LastUpdated"], '%Y-%m-%d %H:%M:%S%z')
    return file_upload_dt


def write_datafile_meta(datafile,path):
    file_upload_dt = get_file_update_time_from_DataCommons(datafile)
    d = {}
    d["LastUpdated"] = format(file_upload_dt)
    data = strip_file_extension(datafile)
    with open(path + "/" + data + '_metadata.json', 'w') as file:
        file.write(json.dumps(d))


def read_datafile_meta(datafile,path):
    data = strip_file_extension(datafile)
    try:
        with open(path + "/" + data + '_metadata.json', 'r') as file:
            file_contents = file.read()
            metadata = json.loads(file_contents)
    except FileNotFoundError:
        log.error("Local metadata file for " + datafile + " is missing.")
    return metadata


def load_flowsa_parquet(parquet,flowsa_type='fba'):
    ""
    if flowsa_type=="fba":
        local_path = fba_local_path
        remote_path = fba_remote_path
    elif flowsa_type=="fbs":
        local_path = fbs_local_path
        remote_path = fbs_remote_path
    local_file = local_path + parquet + ".parquet"
    remote_file = remote_path + parquet + ".parquet"
    try:
        log.debug('Loading ' + parquet + ' parquet from local repository')
        df = pd.read_parquet(local_file)
    except (OSError, FileNotFoundError):
        # if parquet does not exist in local repo, read file from remote
        try:
            log.debug(parquet + ' not found in local folder; loading from Data Commons...')
            df = pd.read_parquet(remote_file)
            df.to_parquet(local_file)
            log.debug(parquet + ' saved in ' + local_path)
        except FileNotFoundError:
            log.error("No file found for " + parquet + " in local or remote server")
    return df