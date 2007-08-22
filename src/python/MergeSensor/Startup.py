#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id: Startup.py,v 1.9 2007/03/28 16:46:13 ckavka Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from MergeSensor.MergeSensorComponent import MergeSensorComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    
    # Basic merge sensor configuration
    compCfg = config.getConfig("MergeSensor")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

    # Local DBS configuration
    localDBSConfig = config.get("LocalDBS")
    if 'ReadDBSURL' in localDBSConfig.keys():
        compCfg["ReadDBSURL"] = localDBSConfig["ReadDBSURL"]  

except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = MergeSensorComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])


                  

