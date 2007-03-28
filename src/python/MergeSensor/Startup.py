#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id: Startup.py,v 1.7 2006/08/25 11:03:59 ckavka Exp $"
__version__ = "$Revision: 1.7 $"
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
    if 'DBSURL' in localDBSConfig.keys():
        compCfg["DBSURL"] = localDBSConfig["DBSURL"]  
    compCfg["DBSAddress"] = localDBSConfig["DBSAddress"]

except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = MergeSensorComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])


                  

