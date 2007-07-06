#!/usr/bin/env python
"""
_ResourceMonitorComponent_

Component that loads a Monitor plugin and invokes it periodically.
If that plugin returns an integer greater than 0,
that many ResourcesAvailable events are generated

"""
import os
import time
import logging
from logging.handlers import RotatingFileHandler

from MessageService.MessageService import MessageService

import ResourceMonitor.Monitors
from ResourceMonitor.Registry import retrieveMonitor


from ProdAgentCore.ResourceConstraint import ResourceConstraint
from ProdAgentCore.PluginConfiguration import PluginConfiguration
import ProdAgentCore.LoggingUtils as LoggingUtils

class ResourceMonitorComponent:
    """
    _ResourceMonitorComponent_

    ProdAgent component that polls looking for available
    resources

    """
    def __init__(self, **args):
        self.args = {}
        self.args['MonitorName'] = None
        self.args['Logfile'] = None
        self.args['MonitorPluginConfig'] = None
        self.args.setdefault("PollInterval", 600 )
        self.args.update(args)
        self.args['PollInterval'] = float(self.args['PollInterval'])
        self.pollDelay = float(self.args['PollInterval'])

        #  //
        # // Default is to start polling resources right away
        #//
        self.activePolling = True

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        LoggingUtils.installLogHandler(self)
        msg = "ResourceMonitor Component Started:\n"
        msg += " ==> Monitor = %s\n" % self.args['MonitorName']
        msg += " ==> PollInterval = %s s\n" % self.args['PollInterval']
        logging.info(msg)
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        logging.debug("Current Monitor: %s" % self.args['MonitorName'])
        
        if event == "ResourceMonitor:Start":
            logging.info("Starting RM...")
            self.activePolling = True
            return

        if event == "ResourceMonitor:Stop":
            logging.info("Stopping RM...")
            self.activePolling = False
            return
            
        if event == "ResourceMonitor:Poll":
            self.pollResources()
            return            

        elif event == "ResourceMonitor:SetMonitor":
            #  //
            # // Payload should be name of registered creator
            #//
            self.setMonitor(payload)
            logging.debug("Set Monitor: %s" % payload)
            return

        elif event == "ResourceMonitor:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "ResourceMonitor:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return


                
    def setMonitor(self, name):
        """
        _setMonitor_

        Allow dynamic changing of Monitor plugin. Sets the MonitorName
        to the value provided.

        """
        self.args['MonitorName'] = name
        return

    def loadMonitor(self):
        """
        _loadMonitor_

        Load the monitor plugin specified by MonitorName

        """
        try:
            monitor = retrieveMonitor(self.args['MonitorName'])
        except StandardError, ex:
            msg = "Failed to load monitor named: %s\n" % (
                self.args['MonitorName'],
                )
            msg += str(ex)
            msg += "\nUnable to poll for resources..."
            logging.error(msg)
            return None

        if self.args['MonitorPluginConfig'] != None:
            monitor.pluginConfiguration = PluginConfiguration()
            try:
                monitor.pluginConfiguration.loadFromFile(
                    self.args['MonitorPluginConfig']
                    )
            except Exception, ex:
                msg = "Unable to load configuration file for plugin:\n"
                msg += "%s\n" % self.args['MonitorPluginConfig']
                msg += str(ex)
                logging.error(msg)
            
        return monitor

    def publishResources(self, constraints):
        """
        _publishResources_

        """
        for constraint in constraints:
            if constraint['count'] == 0:
                continue
            self.ms.publish("ResourcesAvailable", str(constraint))
            self.ms.commit()
            time.sleep(.1)
        return


    def startComponent(self):
        """
        _startComponent_

        Start component, subscribe to messages and start polling thread

        """
       
        # create message server
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("ResourceMonitor")
        self.ms.subscribeTo("ResourceMonitor:Start")
        self.ms.subscribeTo("ResourceMonitor:Stop")
        self.ms.subscribeTo("ResourceMonitor:SetMonitor")
        self.ms.subscribeTo("ResourceMonitor:StartDebug")
        self.ms.subscribeTo("ResourceMonitor:EndDebug")
        self.ms.subscribeTo("ResourceMonitor:Poll")
       
        # generate first polling cycle
        self.ms.remove("ResourceMonitor:Poll")
        self.ms.publish("ResourceMonitor:Poll", "")
        self.ms.commit()
 
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("ResourceMonitor: %s, %s" % (type, payload))
            self.__call__(type, payload)
            
            

    def pollResources(self):
        """
        _pollResources_


        """
        returnValue = 0
        if not self.activePolling:
            logging.debug("pollResources:Inactive")
        else:
            logging.debug("pollResources:Active")
            monitor = self.loadMonitor()
            if monitor != None:
                try:
                    resourceConstraints = monitor()
                except StandardError, ex:
                    msg = "Error invoking monitor:"
                    msg += self.args['MonitorName']
                    msg += "\n%s\n" % str(ex)
                    logging.error(msg)
                    resourceConstr = ResourceConstraint()
                    resourceConstr['count'] = 0
                    resourceConstraints = [resourceConstr]
                logging.debug("%s Resources Available" % resourceConstraints)
                
                self.publishResources(resourceConstraints)
        
        # generate new polling cycle
        self.ms.publish('ResourceMonitor:Poll', '', self.pollDelay)
        self.ms.commit()
       
        return returnValue
    
