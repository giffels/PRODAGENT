#!/usr/bin/env python
"""
_WorkflowInjectorComponent_

ProdAgent Component to process a Workflow with a  plugin
and inject a set of jobs into the PA JobQueue

"""

import os
import logging

from ProdCommon.Database import Session
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgentCore.LoggingUtils as LoggingUtils

import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
import ProdAgent.WorkflowEntities.Job as WEJob


from WorkflowInjector.Registry import retrievePlugin
import WorkflowInjector.Plugins


class WorkflowInjectorComponent:
    """
    _WorkflowInjectorComponent_

    """
    
    def __init__(self, **args):
        
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        self.plugin = None
        if self.args.get("Plugin", None) != None:
            self.plugin = self.args["Plugin"]
        
        
        self.ms = None
        msg = "WorkflowInjector Component Started\n"
        msg += " => Plugin: %s\n" % self.plugin
        logging.info(msg)


    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """
        logging.debug("Message=%s Payload=%s" % (event, payload))

        if event == "WorkflowInjector:SetPlugin":
            self.plugin = payload
            logging.info("Plugin Set to %s" % payload)
            return
        if event == "WorkflowInjector:Input":
            self.handleInput(payload)
            return
        

        if event == "WorkflowInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "WorkflowInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        
        return

    def loadPlugin(self):
        """
        _loadPlugin_

        Load the named plugin

        """
        if self.plugin == None:
            msg = "Plugin Name is not set\n"
            msg += "Unable to load WorkflowInjector Plugin"
            logging.warning(msg)
            return
        try:
            pluginInstance = retrievePlugin(self.plugin)
        except Exception, ex:
            msg = "Unable to load Plugin named %s\n" % self.plugin
            msg += str(ex)
            logging.error(msg)
            return
        return pluginInstance
    

    def handleInput(self, payload):
        """
        _handleInput_

        Handle a new input request

        """
        plugin = self.loadPlugin()
        if plugin == None:
            msg = "Error loading plugin, failed to handle input payload:\n"
            msg += str(payload)
            logging.error(msg)
            return
        plugin.args.update(self.args)

        try:
            plugin(payload)
        except Exception, ex:
            msg = "Error invoking Plugin: %s\n" % self.plugin
            msg += "On Input payload:\n%s\n" % payload
            msg += str(ex)
            logging.error(msg)
            return        
        
        
    


    
    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component
        
        """                                   
        # create message service
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("WorkflowInjector")                                                                                
        # subscribe to messages
        self.ms.subscribeTo("WorkflowInjector:Input")
        self.ms.subscribeTo("WorkflowInjector:StartDebug")
        self.ms.subscribeTo("WorkflowInjector:EndDebug")
        self.ms.subscribeTo("WorkflowInjector:SetPlugin")
        
        
        
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("WFInjector: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)
            Session.commit_all()
            Session.close_all()
            
           
