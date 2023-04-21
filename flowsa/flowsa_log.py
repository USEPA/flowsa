import logging
import shutil
import sys
from esupy.processed_data_mgmt import create_paths_if_missing
from .settings import logoutputpath

try:
    from colorama import init, Fore, Style
except ModuleNotFoundError:
    print('Install colorama for colored log output')
    console_formatter = logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
else:
    init()

    class ColoredFormatter(logging.Formatter):
        FORMATS = {
            logging.DEBUG: logging.Formatter('%(asctime)s ' + Fore.CYAN
                                             + '%(levelname)-8s' + Fore.RESET
                                             + ' %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S'),
            logging.INFO: logging.Formatter('%(asctime)s ' + Fore.GREEN
                                            + '%(levelname)-8s' + Fore.RESET
                                            + ' %(message)s',
                                            datefmt='%Y-%m-%d %H:%M:%S'),
            logging.WARNING: logging.Formatter('%(asctime)s ' + Fore.YELLOW
                                               + '%(levelname)-8s' + Fore.RESET
                                               + ' %(message)s',
                                               datefmt='%Y-%m-%d %H:%M:%S'),
            logging.ERROR: logging.Formatter('%(asctime)s ' + Fore.RED
                                             + '%(levelname)-8s' + Fore.RESET
                                             + ' %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S'),
            logging.CRITICAL: logging.Formatter('%(asctime)s ' + Fore.RED
                                                + Style.BRIGHT
                                                + '%(levelname)-8s'
                                                + Style.RESET_ALL
                                                + ' %(message)s',
                                                datefmt='%Y-%m-%d %H:%M:%S')
        }

        def format(self, record):
            return self.FORMATS.get(record.levelno).format(record)

    console_formatter = ColoredFormatter()

file_formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S')

log_file_handler = logging.FileHandler(
    logoutputpath + 'flowsa.log',
    mode='w', encoding='utf-8')
log_file_handler.setLevel(logging.DEBUG)
log_file_handler.setFormatter(file_formatter)

validation_file_handler = logging.FileHandler(
    logoutputpath + 'flowsa_validation.log',
    mode='w', encoding='utf-8')
validation_file_handler.setLevel(logging.DEBUG)
validation_file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

log = logging.getLogger('flowsa')
log.setLevel(logging.DEBUG)
log.addHandler(console_handler)
log.addHandler(log_file_handler)
log.propagate = False

vlog = logging.getLogger('flowsa.validation')
vlog.setLevel(logging.DEBUG)
vlog.addHandler(validation_file_handler)


def rename_log_file(filename, fb_meta):
    """
    Rename the log file saved to local directory using df meta for df
    :param filename: str, name of dataset
    :param fb_meta: metadata for parquet
    :return: modified log file name
    """
    # original log file name - all log statements
    log_file = f'{logoutputpath}{"flowsa.log"}'
    # generate new log name
    new_log_name = (f'{logoutputpath}{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'.log')
    # create log directory if missing
    create_paths_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)
    # original log file name - validation
    log_file = f'{logoutputpath}{"validation_flowsa.log"}'
    # generate new log name
    new_log_name = (f'{logoutputpath}{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'_validation.log')
    # create log directory if missing
    create_paths_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)
