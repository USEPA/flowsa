import sys
import os
import logging
import subprocess
from esupy.processed_data_mgmt import Paths, create_paths_if_missing
from esupy.util import get_git_hash


try:
    MODULEPATH = \
        os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError:
    MODULEPATH = 'flowsa/'

datapath = MODULEPATH + 'data/'
crosswalkpath = datapath + 'activitytosectormapping/'
externaldatapath = datapath + 'external_data/'
process_adjustmentpath = datapath + 'process_adjustments/'

methodpath = MODULEPATH + 'methods/'
sourceconfigpath = methodpath + 'flowbyactivitymethods/'
flowbysectormethodpath = methodpath + 'flowbysectormethods/'
flowbysectoractivitysetspath = methodpath + 'flowbysectoractivitysets/'

datasourcescriptspath = MODULEPATH + 'data_source_scripts/'

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = os.path.realpath(paths.local_path + "/flowsa")
outputpath = paths.local_path.replace('\\', '/') + '/'
fbaoutputpath = outputpath + 'FlowByActivity/'
fbsoutputpath = outputpath + 'FlowBySector/'
biboutputpath = outputpath + 'Bibliography/'
logoutputpath = outputpath + 'Log/'
diffpath = outputpath + 'FBSComparisons/'
plotoutputpath = outputpath + 'Plots/'
tableoutputpath = outputpath + 'DisplayTables/'

# ensure directories exist
create_paths_if_missing(logoutputpath)
create_paths_if_missing(plotoutputpath)
create_paths_if_missing(tableoutputpath)

DEFAULT_DOWNLOAD_IF_MISSING = False

# paths to scripts
scriptpath = \
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))
                    ).replace('\\', '/') + '/scripts/'
scriptsFBApath = scriptpath + 'FlowByActivity_Datasets/'

# define 4 logs, one for general information, one for major validation
# logs that are also included in the general info log, one for very specific
# validation that is only included in the validation log, and a console
# printout that includes general and validation, but not detailed validation


# format for logging .txt generated
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

# create loggers
# general logger
log = logging.getLogger('allLog')
log.setLevel(logging.DEBUG)
log.propagate = False
# log.propagate=False
# general validation logger
vLog = logging.getLogger('validationLog')
vLog.setLevel(logging.DEBUG)
vLog.propagate = False
# detailed validation logger
vLogDetailed = logging.getLogger('validationLogDetailed')
vLogDetailed.setLevel(logging.DEBUG)
vLogDetailed.propagate = False

# create handlers
# create handler for overall logger
log_fh = logging.FileHandler(logoutputpath+'flowsa.log',
                             mode='w', encoding='utf-8')
log_fh.setFormatter(formatter)
# create handler for general validation information
vLog_fh = logging.FileHandler(logoutputpath+'validation_flowsa.log',
                              mode='w', encoding='utf-8')
vLog_fh.setFormatter(formatter)
# create console handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

# add handlers to various loggers
# general logger
log.addHandler(ch)  # print to console
log.addHandler(log_fh)
vLog.addHandler(log_fh)
# validation logger
vLog.addHandler(ch)  # print to console
vLog.addHandler(vLog_fh)
vLogDetailed.addHandler(vLog_fh)


def return_pkg_version():

    # return version with git describe
    try:
        # set path to flowsa repository, necessary if running method files
        # outside the flowsa repo
        tags = subprocess.check_output(
            ["git", "describe", "--tags", "--always"],
            cwd=MODULEPATH).decode().strip()
        version = tags.split("-", 1)[0].replace('v', "")
    except subprocess.CalledProcessError:
        log.info('Unable to return version with git describe')
        version = 'None'

    return version


# metadata
PKG = "flowsa"
PKG_VERSION_NUMBER = return_pkg_version()
GIT_HASH = get_git_hash()
GIT_HASH_LONG = get_git_hash('long')

# Common declaration of write format for package data products
WRITE_FORMAT = "parquet"
