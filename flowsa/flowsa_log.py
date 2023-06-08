import logging
import shutil
import sys
from esupy.processed_data_mgmt import mkdir_if_missing
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

def get_log_file_handler(name, level=logging.DEBUG):
    h = logging.FileHandler(
        logoutputpath / name,
        mode='w', encoding='utf-8')
    h.setLevel(level)
    h.setFormatter(file_formatter)
    return h

log_file_handler = get_log_file_handler('flowsa.log', logging.INFO)
validation_file_handler = get_log_file_handler('flowsa_validation.log')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

log = logging.getLogger('flowsa')
log.addHandler(console_handler)
log.addHandler(log_file_handler)
log.propagate = False

vlog = logging.getLogger('flowsa.validation')
vlog.setLevel(logging.DEBUG)
vlog.addHandler(validation_file_handler)


def reset_log_file(filename, fb_meta):
    """
    Rename the log file saved to local directory using df meta and
        reset the log
    :param filename: str, name of dataset
    :param fb_meta: metadata for parquet
    """
    # original log file name - all log statements
    log_file = logoutputpath / "flowsa.log"
    # generate new log name
    new_log_name = (logoutputpath / f'{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'.log')
    # create log directory if missing
    mkdir_if_missing(logoutputpath)
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)

    # Reset log file
    for h in log.handlers:
        if isinstance(h, logging.FileHandler):
            log.removeHandler(h)
    log.addHandler(get_log_file_handler('flowsa.log', logging.INFO))

    if fb_meta.category == 'FlowByActivity':
        return

    # original log file name - validation
    log_file = logoutputpath / "flowsa_validation.log"
    # generate new log name
    new_log_name = (logoutputpath / f'{filename}_v'
                    f'{fb_meta.tool_version}'
                    f'{"_" + fb_meta.git_hash if fb_meta.git_hash else ""}'
                    f'_validation.log')
    # rename the standard log file name (os.rename throws error if file
    # already exists)
    shutil.copy(log_file, new_log_name)

    # Reset validation log file
    for h in vlog.handlers:
        if isinstance(h, logging.FileHandler):
            vlog.removeHandler(h)
    vlog.addHandler(get_log_file_handler('flowsa_validation.log'))
