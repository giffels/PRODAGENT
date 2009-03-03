#!/usr/bin/env python
"""
_RestartBot_

Automatically restart any components that are not running based on the ProdAgent Config contents

"""
import sys
import logging
import os
import inspect

from AdminControl.Bots.BotInterface import BotInterface
from AdminControl.Registry import registerBot

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails





class RestartBot(BotInterface):
    """
    _RestartBot_

    When invoked, scan the ProdAgentConfig, check all component processes are running, and if they are not,
    restart that component.
    
    """
    def __init__(self):
        BotInterface.__init__(self)
        self.skipCycles = 2



    def __call__(self):
        """
        _operator()_

        Invoke the Bot to check each component

        """
        cfgObject = loadProdAgentConfiguration()
        components = cfgObject.listComponents()
        restartedComponents = []
        for component in components:
            logging.info("RestartBot: Checking %s" % component)
            compCfg = cfgObject.getConfig(component)
            compDir = compCfg['ComponentDir']
            compDir = os.path.expandvars(compDir)
            daemonXml = os.path.join(compDir, "Daemon.xml")
            doRestart = False
            if not os.path.exists(daemonXml):
                doRestart = True
                
            daemon = DaemonDetails(daemonXml)
            if not daemon.isAlive():
                doRestart = True

            if doRestart:
                restartedComponents.append(component)
                modRef = __import__(component, globals(), locals(), [])
                srcFile = inspect.getsourcefile(modRef)
                srcDir = os.path.dirname(srcFile)
                startup = os.path.join(srcDir, "Startup.py")
                if not os.path.exists(startup):
                    msg = "Error starting component: %s\n" % component
                    msg += "Startup file is not found:\n"
                    msg += "  %s \n" % startup
                    logging.warning(msg)
                    continue

                logging.info( "Starting Component %s:" % component)
                logging.info( "With: %s" % startup)
                os.system("%s %s " % (sys.executable, startup))
            else:
                logging.info("RestartBot: Component %s Running" % component)
        #  //
        # // Mail notification
        #//
        if len(restartedComponents) > 0 :
            mailMsg = "RestartBot. The following components were not running. The have been restarted:\n"
            for component in restartedComponents :
                mailMsg += " %s" % (component)
            mailMsg += "\nYours, RestartBot."
            self.mail(mailMsg)
        return


registerBot(RestartBot, RestartBot.__name__)
