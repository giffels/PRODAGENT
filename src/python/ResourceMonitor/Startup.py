#!/usr/bin/env python
"""
_StartComponent_

Start the RequestInjector component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ResourceMonitor.ResourceMonitorComponent import ResourceMonitorComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ResourceMonitor")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//
print "Starting ResourceMonitor Component..."
createDaemon(compCfg['ComponentDir'])
component = ResourceMonitorComponent(**dict(compCfg))
component.startComponent()
