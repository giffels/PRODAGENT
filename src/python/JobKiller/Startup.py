#!/usr/bin/env python
"""
_StartComponent_

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from JobKiller.JobKillerComponent import JobKillerComponent
from ProdAgentCore.PostMortem import runWithPostMortem

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("JobKiller")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

#  //
# // Get BOSS configuration
#//

try:
    bossCfg = config.getConfig("BOSS")
    compCfg['bossConfigDir'] = bossCfg['configDir']
except StandardError, ex:
    compCfg['bossConfigDir'] = None

#  //
# // Set component directory
#//

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//

print "Starting JobKiller Component..."
createDaemon(compCfg['ComponentDir'])
component = JobKillerComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])
