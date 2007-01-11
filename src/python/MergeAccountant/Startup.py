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
from MergeAccountant.MergeAccountantComponent import MergeAccountantComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    
    # Basic merge accountant configuration
    compCfg = config.getConfig("MergeAccountant")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
 
    # Add merge sensor configuration
    MergeSensorConfig = config.get("MergeSensor")
    compCfg["MaxInputAccessFailures"] = \
                    MergeSensorConfig["MaxInputAccessFailures"]
   
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = MergeAccountantComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])


                  

