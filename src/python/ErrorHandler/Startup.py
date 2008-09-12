#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ErrorHandler.ErrorHandlerComponent import ErrorHandlerComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ErrorHandler")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

if os.environ.get("PRODAGENT_WORKDIR", None) == None:
    msg = "ProdAgent environment not initialised properly"
    msg += "$PRODAGENT_WORKDIR is not set"
    raise RuntimeError, msg

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
compCfg['jobReportLocation'] = os.path.expandvars(compCfg['jobReportLocation']) 
#  //
# // Initialise and start the component
#//
createDaemon(compCfg['ComponentDir'])
component = ErrorHandlerComponent(**dict(compCfg))
component.startComponent()
