#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id: Startup.py,v 1.4.14.3 2008/03/28 15:35:25 gcodispo Exp $"

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from JobTracking.TrackingComponent import TrackingComponent
from ProdAgentCore.PluginConfiguration import loadPluginConfig

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("JobTracking")

    # BOSS configuration # deprecated since BossLite
    # bossConfig = config.get("BOSS")
    # if 'configDir' in bossConfig.keys():
    #    compCfg['configDir'] = bossConfig['configDir']

    # ProdAgent configuration
    paConfig = config.get("ProdAgent")
    if 'ProdAgentWorkDir' in paConfig.keys():
        compCfg['ProdAgentWorkDir'] = paConfig['ProdAgentWorkDir']

    try:
        # ProdAgent configuration
        jobCreatorConfig = config.get("JobCreator")
        if 'ComponentDir' in jobCreatorConfig.keys():
            compCfg['JobCreatorComponentDir'] = \
                                              jobCreatorConfig['ComponentDir']
    except AttributeError:
        compCfg['JobCreatorComponentDir'] = ""

    try:

        # get dashboard information from submitter configuration plugin
        pluginConfig = loadPluginConfig("JobSubmitter", "Submitter")
        dashboardCfg = pluginConfig.get('Dashboard', {})

        # build dashboard info structure
        dashboardInfo = {}
        dashboardInfo['use'] = dashboardCfg["UseDashboardINFO"]
        dashboardInfo['address'] = dashboardCfg["DestinationHost"]
        dashboardInfo['port'] = dashboardCfg["DestinationPort"]

        # store it
        compCfg["dashboardInfo"] = dashboardInfo

    except Exception:

        # problems, accept the default one
        pass

except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    import traceback
    msg += traceback.format_exc()
    raise RuntimeError, msg

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

#  //
# // Initialise and start the component
#//

createDaemon(compCfg['ComponentDir'])
component = TrackingComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])
