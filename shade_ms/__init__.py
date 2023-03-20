import logging
import os

# set default number of renderers to half the available cores
DEFAULT_CNUM = 16
DEFAULT_NUM_RENDERS = max(1, os.cpu_count()//2)

from .__main__ import cli, parse_plot_spec, parse_slice_spec  # noqa

# create logger with 'spam_application'
log = logging.getLogger('shadems')
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
log_file_handler = logging.FileHandler('log-shadems.txt')
log_file_handler.setLevel(logging.DEBUG)
# create console handler with a higher log level
log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                              "%Y-%m-%d %H:%M:%S")
log_file_handler.setFormatter(formatter)
log_console_handler.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(log_file_handler)
log.addHandler(log_console_handler)


def separator():
    log.info('------------------------------------------------------')
