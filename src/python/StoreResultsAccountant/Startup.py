#!/usr/bin/env python
"""
_StartComponent_

Start the RelValInjector component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from StoreResultsAccountant.StoreResultsAccountantComponent import StoreResultsAccountantComponent

#  //
# // Find and load the Configuration
#//

try:

    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("StoreResultsAccountant")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#
#  Initialise and start the component
#

print "Starting StoreResultsAccountant Component..."
createDaemon(compCfg['ComponentDir'])
component = StoreResultsAccountantComponent(**dict(compCfg))

runWithPostMortem(component, compCfg['ComponentDir'])
