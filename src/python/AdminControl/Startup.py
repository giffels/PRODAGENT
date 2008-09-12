#!/usr/bin/env python
"""
_StartComponent_

Start the AdminControl component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from AdminControl.AdminControlComponent import AdminControlComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("AdminControl")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

if os.environ.get("PRODAGENT_WORKDIR", None) == None:
    msg = "ProdAgent environment not initialised properly"
    msg += "$PRODAGENT_WORKDIR is not set"
    raise RuntimeError, msg

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//
print "Starting AdminControl Component..."
createDaemon(compCfg['ComponentDir'])
component = AdminControlComponent(**dict(compCfg))
component.startComponent()
