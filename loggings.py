import logging
from logging.config import fileConfig
import os

BASE = os.path.dirname(os.path.abspath(__file__))
CONF_DIR = os.path.join(BASE, "confs")


class LoggableMixin(object):

    def __init__(self):
        fileConfig(os.path.join(CONF_DIR, "logging_config.ini"))
        self.logger = logging.getLogger()
