#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import os

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from Monitoring.MonitoringComponent import MonitoringComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    
    # Basic configuration
    compCfg = config.getConfig("Monitoring")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

    # Get ProdAgent name
    prodAgentConfig = config.getConfig("ProdAgent")
    compCfg['ProdAgentName'] = prodAgentConfig['ProdAgentName']

    # get database parameters
    prodAgentDB = config.getConfig("ProdAgentDB")
    compCfg['passwd'] = prodAgentDB['passwd']
    compCfg['host'] = prodAgentDB['host']
    compCfg['user'] = prodAgentDB['user']
    compCfg['socketFileLocation'] = prodAgentDB['socketFileLocation']
    compCfg['portNr'] = prodAgentDB['portNr']
    compCfg['dbName'] = prodAgentDB['dbName']

except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = MonitoringComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])


                  

