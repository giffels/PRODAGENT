#!/usr/bin/env python
"""
WMAgent Configuration

Configuration used for running the DBSUpload WMAgent component in the Tier1
Skimming system.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.1 2009/05/22 15:51:16 sfoulkes Exp $"
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
config.CoreDatabase.dialect = "mysql"
#config.CoreDatabase.socket = "/storage/local/data1/pa/T0/dev/WMAgent/mysqldata/mysql.sock"
config.CoreDatabase.socket = "/storage/local/data1/pa/T0/dev/prodAgent/mysqldata/mysql.sock"
config.CoreDatabase.hostname = "localhost"
config.CoreDatabase.masterUser = "root"
config.CoreDatabase.masterPasswd = "98passwd"
config.CoreDatabase.user = "root"
config.CoreDatabase.passwd = "98passwd"
config.CoreDatabase.name = "WMAgentDB"

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.ComponentDir = config.General.workDir + "/Components/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
config.DBSUpload.bufferSuccessHandler = "WMComponent.DBSUpload.Handler.BufferSuccess"
config.DBSUpload.newWorkflowHandler = "WMComponent.DBSUpload.Handler.NewWorkflowHandler"
config.DBSUpload.dbsurl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
config.DBSUpload.dbsversion = "DBS_2_0_6"
config.DBSUpload.uploadFileMax = 10


