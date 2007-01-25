#!/usr/bin/env python
"""
_AdminControlComponent_

Component that provides an admin interface API and an automated Bot system
for running periodic maintainence Bots.

"""

import os
import logging
import time

from logging.handlers import RotatingFileHandler
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils

from AdminControl.Registry import retrieveBot
import AdminControl.Bots

class AdminControlComponent:
    """
    _AdminControlComponent_

    ProdAgent component that provides monitoring and XMLRPC API interface
    for monitoring and interacting with the live ProdAgent

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['Bots'] = ""
        self.args['BotPeriod'] = "01:00:00"
        self.args.update(args)

        self.botList = []
        self.bots = {}
        self.stopBots = False
            
        
        for botname in self.args['Bots'].split(','):
            if len(botname.strip()) > 0:
                self.botList.append(botname.strip())
                
        
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        msg = "AdminControl Component Started"
        msg += " => Bot Update Period: %s\n " % self.args['BotPeriod']
        msg += " => Bots: %s\n " % self.botList
        logging.info(msg)

    def __call__(self, event, payload):
        """
        _operator()_

        Event handler method

        """
        if event == "AdminControl:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "AdminControl:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        if event == "AdminControl:StartBots":
            self.startBots()
            return
        if event == "AdminControl:StopBots":
            self.stopBots()
            return

        if event == "AdminControl:ActivateBot":
            self.activateBot(payload)
            return
        if event == "AdminControl:DeactivateBot":
            self.deactivateBot(payload)
            return
        if event == "AdminControl:BotCycle":
            self.botCycle()
            return

        return
        
    def startBots(self):
        """
        _startBots_

        Load and Start all Bots
        
        """
        if len(self.bots.keys()) > 0:
            msg = "Bots already started, cannot start Bots"
            logging.error(msg)

        for bot in self.botList:
            try:
                newBot = retrieveBot(bot)
                logging.info("Bot Started: %s" % bot)
            except Exception, ex:
                msg = "Error retrieving Bot: %s\n" % bot
                msg += str(ex)
                logging.error(msg)
                continue
            self.bots[bot] = newBot

        self.ms.publish("AdminControl:BotCycle", "", self.args['BotPeriod'])
        self.ms.commit()
        return

    def stopBots(self):
        """
        _stopBots_

        Shut Down and delete Bot Instances

        """
        self.stopBots = True
        for botname in self.bots.keys():
            logging.info("Bot Stopped: %s" % botname)
            del self.bots[botname]
        return
                       
    
    def activateBot(self, botname):
        """
        _activateBot_

        Set named Bot to Active State
        """
        botRef = self.bots.get(botname, None)
        if botRef == None:
            msg = "Unable to get Bot named: %s\n" % botname
            msg += "Bot Not Activated"
            logging.error(msg)
            return
        botRef.active = True
        logging.info("Bot Activated: %s" % botname)
        return
    

    def deactivateBot(self, botname):
        """
        _deactivateBot_

        Set named Bit to Inactive state si that it doesnt get run

        """
        botRef = self.bots.get(botname, None)
        if botRef == None:
            msg = "Unable to get Bot named: %s\n" % botname
            msg += "Bot Not Deactivated"
            logging.error(msg)
            return
        botRef.active = False
        logging.info("Bot Deactivated: %s" % botname)
        return

    def botCycle(self):
        """
        _botCycle_

        Run one bot cycle

        """
        if self.stopBots:
            self.stopBots = False
            return
        for bot in self.bots.values():
            bot.run()
        self.ms.publish("AdminControl:BotCycle", "", self.args['BotPeriod'])
        self.ms.commit()
        return
    
    
    def startComponent(self):
        """
        _startComponent_

        """
        self.ms = MessageService()
        # register
        self.ms.registerAs("AdminControl")
        
        # subscribe to messages
        self.ms.subscribeTo("AdminControl:StartDebug")
        self.ms.subscribeTo("AdminControl:EndDebug")
        self.ms.subscribeTo("AdminControl:StartBots")
        self.ms.subscribeTo("AdminControl:StopBots")
        self.ms.subscribeTo("AdminControl:ActivateBot")
        self.ms.subscribeTo("AdminControl:DeactivateBot")
        self.ms.subscribeTo("AdminControl:BotCycle")

        self.startBots()
        
        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.__call__(messageType, payload)
            self.ms.commit()
        

