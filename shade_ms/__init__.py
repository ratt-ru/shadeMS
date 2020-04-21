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

class IndentingLogFormatter(logging.Formatter):
    """A formatter that pads log messages that start with ":" out to a specified indent"""
    def __init__(self, *args, **kw):
        logging.Formatter.__init__(self, *args, **kw)
        self.set_indent(17)  # field of 16 plus space

    def set_indent(self, num):
        self.indent = " "*num

    def format(self, record):
        if record.msg.strip().startswith(":"):
            record.msg = self.indent + record.msg.strip()
        return logging.Formatter.format(self, record)

formatter = IndentingLogFormatter("{asctime} - {levelname:8} - {message}", "%Y-%m-%d %H:%M:%S", "{")
log_file_handler.setFormatter(formatter)
log_console_handler.setFormatter(formatter)

# add the handlers to the logger
log.addHandler(log_file_handler)
log.addHandler(log_console_handler)


def blank():
    log.info('------------------------------------------------------')
