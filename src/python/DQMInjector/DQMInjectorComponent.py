#!/usr/bin/env python
"""
_DQMInjectorComponent_

Create Offline DQM Histogram collection jobs in an automated manner
The component will poll a data source to retrieve a list of files
in a run within a dataset and generate a job spec to pull the DQM
histograms out of the file and post them to the DQM Server.
"""

import os
import logging
import traceback

from ProdCommon.Database import Session
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgentCore.LoggingUtils as LoggingUtils
import ProdAgent.WorkflowEntities.Job as WEJob
from JobQueue.JobQueueAPI import bulkQueueJobs

from DQMInjector.CollectPayload import CollectPayload
from DQMInjector.Plugins.DBSPlugin import DBSPlugin
from DQMInjector.Plugins.T0ASTPlugin import T0ASTPlugin
from DQMInjector.Plugins.RelValPlugin import RelValPlugin

class DQMInjectorComponent:
    """
    _DQMInjectorComponent_
    """

    def __init__(self, **args):

        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args['Plugin'] = "DBSPlugin"
        self.args['ScramArch'] = None
        self.args['CmsPath'] = None
        self.args['ConfigFile'] = None
        self.args['OverrideCMSSW'] = None
        self.args['OverrideGlobalTag'] = None
        self.args['DQMServer'] = None
        self.args['proxyLocation'] = None
        self.args['DQMCopyToCERN'] = None
        self.args['Site'] = "srm-cms.cern.ch"
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)
        self.pluginRegistry = {}
        self.pluginRegistry['DBSPlugin'] = DBSPlugin
        self.pluginRegistry['T0ASTPlugin'] = T0ASTPlugin
        self.pluginRegistry['RelValPlugin'] = RelValPlugin

        if self.args['ScramArch'] == None:
            self.args['ScramArch'] = os.environ.get("SCRAM_ARCH", None)
        if self.args['CmsPath'] == None:
            self.args['CmsPath'] = os.environ.get("CMS_PATH", None)
            

        self.ms = None
        msg = "DQMInjector Component Started\n"
        msg += " => Plugin: %s\n" % self.args['Plugin']
        msg += " => Cms Path: %s\n" % self.args['CmsPath']
        msg += " => Scram Arch: %s\n" % self.args['ScramArch']
        msg += " => Site: %s\n" % self.args['Site']
        if self.args['ConfigFile'] != None:
            msg += " => ConfigFile : %s\n" % self.args['ConfigFile']
        else:
            msg += " => Configurations Generated on the fly"
        if self.args['OverrideCMSSW'] != None:
            msg += " => CMSSW Version Overridden: %s\n" % (
                self.args['OverrideCMSSW'],)
        else:
            msg += " => CMSSW Version looked up by Plugin\n"
        if self.args['OverrideGlobalTag'] != None:
            msg += " => GlobalTag Version Overridden: %s\n" % (
                self.args['OverrideGlobalTag'],)
        else:
            msg += " => GlobalTag Version looked up by Plugin\n"
        msg += " => Server: %s\n" % (self.args['DQMServer'])
        msg += " => proxyLocation: %s\n" % (self.args['proxyLocation'])
        msg += " => Copy to CERN: %s\n" % (self.args['DQMCopyToCERN'])
            
        logging.info(msg)


    def parsePayload(self, payload):
        """
        _parsePayload_

        Extract dataset and run information from a Collect payload

        """
        collectPayload = CollectPayload()
        collectPayload.parse(payload)
        return collectPayload


    def getPlugin(self):
        """
        _getPlugin_

        instantiate a plugin instance

        """
        pluginClass = self.pluginRegistry.get(self.args['Plugin'], None)
        if pluginClass == None:
            msg = "No plugin class named: %s\n" % self.args['Plugin']
            logging.error(msg)
            raise RuntimError, msg
        return pluginClass()


    def collect(self, collectPayload):
        """
        _collect_

        Generate a collection workflow and jobs based on
        the collectPayload provided

        """
        try:
            plugin = self.getPlugin()
            plugin.args.update(self.args)
            plugin.msRef = self.ms
        except Exception, ex:
            msg = "Error creating plugin instance of type: %s\n" % (
                self.args['Plugin'],
                )
            msg += "%s\n" % str(ex)
            msg += "Unable to collect for %s\n" % str(collectPayload)
            logging.error(msg)
            return



        try:
            jobs = plugin(collectPayload)
        except Exception, ex:
            msg = "Error invoking %s plugin on collect payload:\n%s" % (
                self.args['Plugin'], str(collectPayload))
            msg += "\n%s\n" % str(ex)
            msg += traceback.format_exc()
            logging.error(msg)
            return


        #  //
        # // publish and queue harvesting jobs
        #//
        for job in jobs:
            logging.info("Registering Job %s" % job['JobSpecId'])
            WEJob.register(job['WorkflowSpecId'], None, {
                'id' : job['JobSpecId'], 'owner' : 'DQMInjector',
                'job_type' : "Processing", "max_retries" : 3,
                "max_racers" : 1,
                })

        if len(jobs) > 0:
            site = self.args['Site']
            bulkQueueJobs(site, *jobs)
            
        return



    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """
        logging.info("Message=%s Payload=%s" % (event, payload))

        if event == "DQMInjector:Collect":
            collectPayload = self.parsePayload(payload)
            self.collect(collectPayload)
            return

        if event == "DQMInjector:SetPlugin":
            self.args['Plugin'] = payload
            logging.info("Plugin set to %s" % self.args['Plugin'])
            return

        if event == "DQMInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        if event == "DQMInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return

    def startComponent(self):
        """
        _startComponent_

        Start the servers required for this component

        """
        # create message service
        self.ms = MessageService()

        # register
        self.ms.registerAs("DQMInjector")
        # subscribe to messages
        self.ms.subscribeTo("DQMInjector:Collect")
        self.ms.subscribeTo("DQMInjector:SetPlugin")

        self.ms.subscribeTo("DQMInjector:StartDebug")
        self.ms.subscribeTo("DQMInjector:EndDebug")

        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("DQMInjector: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)
            Session.commit_all()
            Session.close_all()






