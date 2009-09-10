#!/usr/bin/env python
"""
WMAgent Configuration

Configuration used for running the DBSUpload WMAgent component in the Tier1
Skimming system.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.1 2009/07/01 21:09:09 dmason Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv60.fnal.gov"
config.Agent.contact = "president@whitehouse.gov"
config.Agent.teamName = "Batavia RickRollers"
config.Agent.agentName = "Tier1Skimmer"

config.section_("General")
config.General.workDir = "/storage/local/data1/pa/T0/dev/WMAgent"

config.section_("CoreDatabase")
config.CoreDatabase.dialect = "Oracle"
config.CoreDatabase.hostname = "cmswmbs"
config.CoreDatabase.masterUser = "tier1_wmbs"
config.CoreDatabase.masterPasswd = ""
config.CoreDatabase.user = "tier1_wmbs"
config.CoreDatabase.passwd = ""
config.CoreDatabase.tablespaceName = "TIER1_WMBS_DATA"
config.CoreDatabase.indexspaceName = "TIER1_WMBS_INDEX"

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.ComponentDir = config.General.workDir + "/Components/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
config.DBSUpload.bufferSuccessHandler = "WMComponent.DBSUpload.Handler.BufferSuccess"
config.DBSUpload.newWorkflowHandler = "WMComponent.DBSUpload.Handler.NewWorkflowHandler"
config.DBSUpload.dbsurl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
config.DBSUpload.dbsversion = "DBS_2_0_8"
config.DBSUpload.uploadFileMax = 10
config.DBSUpload.pollInterval = 100
config.DBSUpload.DBSMaxSize = 10000000000
config.DBSUpload.DBSMaxFiles = 3

config.component_("PhEDExInjector")
config.PhEDExInjector.namespace = "WMComponent.PhEDExInjector.PhEDExInjector"
config.PhEDExInjector.ComponentDir = config.General.workDir + "/Components/PhEDExInjector"
config.PhEDExInjector.logLevel = "DEBUG"
config.PhEDExInjector.maxThreads = 1
config.PhEDExInjector.phedexurl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
config.PhEDExInjector.pollInterval = 100
