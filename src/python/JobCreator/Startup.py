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
from ProdAgentCore.PostMortem import runWithPostMortem
from JobCreator.JobCreatorComponent import JobCreatorComponent

#  //
# // Find and load the Configuration
#//

try:
    
    config = loadProdAgentConfiguration()
    jobStatesCfg = config.getConfig("JobStates")
    compCfg = config.getConfig("JobCreator")
    #NOTE: this works only if we assume that there are no
    #NOTE: duplicate names in the different configurations
    compCfg.update(jobStatesCfg)
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//
print "Starting JobCreator Component..."
createDaemon(compCfg['ComponentDir'])
component = JobCreatorComponent(**dict(compCfg))

runWithPostMortem(component, compCfg['ComponentDir'])

