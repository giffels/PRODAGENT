#!/usr/bin/env python
"""
_Startup_

Start the RepackerInjector component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from RepackerInjector.RepackerInjectorComponent import RepackerInjectorComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("RepackerInjector")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//
print "Starting RepackerInjector Component..."
createDaemon(compCfg['ComponentDir'])
component = RepackerInjectorComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])
