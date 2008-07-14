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
from JobCleanup.JobCleanupComponent import JobCleanupComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("JobCleanup")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
compCfg['jobReportLocation'] = compCfg['ComponentDir']+'/JobReports' 
#  //
# // Initialise and start the component
#//
createDaemon(compCfg['ComponentDir'])
component = JobCleanupComponent(**dict(compCfg))
component.startComponent()
