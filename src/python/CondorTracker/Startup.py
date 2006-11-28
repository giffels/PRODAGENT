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
from CondorTracker.CondorTrackerComponent import CondorTrackerComponent
from ProdAgentCore.PostMortem import runWithPostMortem

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("CondorTracker")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//
print "Starting CondorTracker Component..."
createDaemon(compCfg['ComponentDir'])
component = CondorTrackerComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])
