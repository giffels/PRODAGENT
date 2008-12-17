#!/usr/bin/env python
"""
Util to test the JobTracking component startup for development by
starting the component as an interactive process so that you can get
stdout/stderr etc

"""
__revision__ = "$Id: TestComponent.py,v 1.4 2008/07/25 15:47:41 swakef Exp $"

from GetOutput.GetOutputComponent import GetOutputComponent
from ProdAgentCore.Configuration import loadProdAgentConfiguration


try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("GetOutput")

    # ProdAgent configuration
    paConfig = config.get("ProdAgent")
    if 'ProdAgentWorkDir' in paConfig.keys():
        compCfg['ProdAgentWorkDir'] = paConfig['ProdAgentWorkDir']

    # JobTracking configuration
    jtConfig = config.get("JobTracking")
    if 'ComponentDir' in jtConfig.keys():
        compCfg['JobTrackingDir'] = jtConfig['ComponentDir']

    # JobCreator configuration
    jobCreatorConfig = config.get("JobCreator")
    if jobCreatorConfig is None:
        compCfg['JobCreatorComponentDir'] = jtConfig['ComponentDir']
    elif 'ComponentDir' in jobCreatorConfig.keys():
        compCfg['JobCreatorComponentDir'] = jobCreatorConfig['ComponentDir']

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

    ## TODO temporary for OSB rebounce # Fabio
    try:
        compCfg.update( config.get("CrabServerConfigurations") )
    except Exception, e:
        pass
    ##


    comp = GetOutputComponent(**dict(compCfg))
    comp.startComponent()

except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    import traceback
    print traceback.format_exc()

print 'bye'
