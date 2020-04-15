import logging

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
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s","%Y-%m-%d %H:%M:%S")
log_file_handler.setFormatter(formatter)
log_console_handler.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(log_file_handler)
log.addHandler(log_console_handler)
