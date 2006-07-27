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
    compCfg["DBSType"] = localDBSConfig["DBSType"]

    # DBS Interface configuration
    DBSInterfaceConfig = config.get("DBSInterface")
    compCfg["DBSDataTier"] = DBSInterfaceConfig["DBSDataTier"]

    # DLS Interface configuration
    DLSInterfaceConfig = config.get("DLSInterface")
    compCfg["DLSType"] = DLSInterfaceConfig["DLSType"]
    compCfg["DLSAddress"] = DLSInterfaceConfig["DLSAddress"]
   
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = MergeSensorComponent(**dict(compCfg))
component.startComponent()
