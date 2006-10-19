#!/usr/bin/env python
"""
_LoggingUtils_

Common logging setup for all components

"""

import logging
from logging.handlers import RotatingFileHandler


def installLogHandler(componentRef):
    """
    _installLogHandler_

    Setup the logging handlers for a component
    Read arguments from the component configuration for component by component customisation


    """

    logSize = componentRef.args.get("LogSize", 1000000)
    logSize = int(logSize)
    logRotate = componentRef.args.get("LogRotate", 3)
    logRotate = int(logRotate)

    defaultLevel = componentRef.args.get("LogLevel", "info")
    loggingLevel = logging.INFO
    
    if defaultLevel.lower() == "debug":
        loggingLevel = logging.DEBUG
    
    logHandler = RotatingFileHandler(componentRef.args['Logfile'],
                                     "a", logSize, logRotate)
    logFormatter = logging.Formatter("%(asctime)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)
    logging.getLogger().setLevel(loggingLevel)
    return
    
