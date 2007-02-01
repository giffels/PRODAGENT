#!/usr/bin/env python
"""
_JobKillerComponent_

ProdAgent Component that kills jobs by job spec or workflow Id

"""
__version__ = "$Revision$"
__revision__ = "$Id$"
__author__ = "evansde@fnal.gov"


import os
import logging


import ProdAgentCore.LoggingUtils as LoggingUtils
from MessageService.MessageService import MessageService

from JobKiller.Registry import retrieveKiller

import JobKiller.Killers

class JobKillerComponent:
    """
    _JobKillerComponent_

    Component that kills jobs without allowing resubmits

    """
    def __init__(self, **args):
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args['KillerName'] = None
        self.args.update(args)
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        LoggingUtils.installLogHandler(self)
        self.ms = None

        msg = "JobKiller Component Started...\n"
        msg += " => Killer Plugin: %s\n" % self.args['KillerName']
        logging.info(msg)
        
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define call for this object to allow it to handle events that
        it is subscribed to
        """
        logging.debug("Event: %s Payload: %s" % (event, payload))
        if event == "JobKiller:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "JobKiller:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        if event == "JobKiller:SetKiller":
            self.args['KillerName'] = payload
            logging.info("Killer Set To: %s" % payload)
            return
        
        if event == "KillJob":
            self.killJob(payload)
            return

        if event == "KillWorkflow":
            self.killWorkflow(payload)
            return

        if event == "EraseJob":
            self.eraseJob(payload)
            return

        if event == "EraseWorkflow":
            self.eraseWorkflow(payload)
            return
            
        
        return


    def eraseJob(self, jobSpecId):
        """
        _eraseJob_

        Remove job in its entirity from ProdAgent
        Will not result in JobFailed event
        
        """
        logging.info("Erasing Job: %s" % jobSpecId)
        killer = self.loadKiller()
        if killer == None:
            msg = "Problem Loading Killer Plugin, unable to erase job: %s" % (
                jobSpecId,
                )
            logging.error(msg)
            return

        try:
            killer.eraseJob(jobSpecId)
        except Exception, ex:
            msg = "Error invoking kill Workflow on %s\n" % jobSpecId
            msg += "With plugin: %s\n" % self.args['KillerName']
            msg += "%s\n" % str(ex)
            logging.error(msg)

        return

    def eraseWorkflow(self, workflowSpecId):
        """
        _eraseWorkflow_

        Remove all jobs for workflow from ProdAgent
        Will not result in JobFailed events
        
        """
        logging.info("Erasing Workflow: %s" % workflowSpecId)
        killer = self.loadKiller()
        if killer == None:
            msg = "Problem Loading Killer Plugin, unable to erase "
            msg += "workflow: %s" % (
                workflowSpecId,
                )
            logging.error(msg)
            return
        try:
            killer.eraseWorkflow(workflowSpecId)
        except Exception, ex:
            msg = "Error invoking erase Workflow on %s\n" % workflowSpecId
            msg += "With plugin: %s\n" % self.args['KillerName']
            msg += "%s\n" % str(ex)
            logging.error(msg)


        return


    def killJob(self, jobSpecId):
        """
        _killJob_
        
        Kill running jobs and prevent resubmit, will result in a
        JobFailed event
        
        """
        logging.info("Killing Job: %s" % jobSpecId)
        killer = self.loadKiller()
        if killer == None:
            msg = "Problem Loading Killer Plugin, unable to kill job: %s" % (
                jobSpecId,
                )
            logging.error(msg)
            return
        try:
            killer.killJob(jobSpecId)
        except Exception, ex:
            msg = "Error invoking kill Job on %s\n" % jobSpecId
            msg += "With plugin: %s\n" % self.args['KillerName']
            msg += "%s\n" % str(ex)
            logging.error(msg)


        return
    
    def killWorkflow(self, workflowSpecId):
        """
        _killWorkflow_
        
        Kill all running jobs for workflow and prevent resubmit,
        will result in JobFailed events
        
        """
        logging.info("Killing Workflow: %s" % workflowSpecId)
        killer = self.loadKiller()
        if killer == None:
            msg = "Problem Loading Killer Plugin, unable to kill "
            msg += "workflow: %s" % (
                workflowSpecId,
                )
            logging.error(msg)
            return

        try:
            killer.killWorkflow(workflowSpecId)
        except Exception, ex:
            msg = "Error invoking kill Workflow on %s\n" % workflowSpecId
            msg += "With plugin: %s\n" % self.args['KillerName']
            msg += "%s\n" % str(ex)
            logging.error(msg)
        
        return


    def loadKiller(self):
        """
        _loadKiller_

        Load the killer plugin

        """
        if self.args['KillerName'] == None:
            msg = "No Killer Plugin selected"
            logging.error(msg)
            return None
        try:
            killer = retrieveKiller(self.args['KillerName'])
        except Exception, ex:
            msg = "Exception when loading Killer Plugin: %s\n" % (
                self.args['KillerName'],)
            msg += str(ex)
            logging.error(msg)
            killer = None
        return killer
    
        
        
            

    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component

        """                                   
        # create message service
        self.ms = MessageService()

        # register
        self.ms.registerAs("JobKiller")
        
        # subscribe to messages
        
        self.ms.subscribeTo("JobKiller:StartDebug")
        self.ms.subscribeTo("JobKiller:EndDebug")
        self.ms.subscribeTo("JobKiller:SetKiller")
        self.ms.subscribeTo("KillJob")
        self.ms.subscribeTo("EraseJob")
        self.ms.subscribeTo("KillWorkflow")
        self.ms.subscribeTo("EraseWorkflow")
        
        # wait for messages
        while True:
            msgtype, payload = self.ms.get()
            self.ms.commit()
            self.__call__(msgtype, payload)



