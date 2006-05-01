#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id: TrackingComponent.py,v 1.8 2006/05/01 11:45:04 elmer Exp $"

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from JobTracking.TrackingComponent import TrackingComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("JobTracking")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg



compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//

createDaemon(compCfg['ComponentDir'])
component = TrackingComponent(**dict(compCfg))
component.startComponent()
