#!/usr/bin/env python
"""
_AdminControlComponent_

Component that provides an admin interface API and an automated Bot system
for running periodic maintainence Bots.

"""

import os
import logging
import time


from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig

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
        self.args['MailTo'] = None
        self.args['SendMail'] = False
        self.args['MaxDiskPercent'] = None
        self.args['MinDiskSpaceGB'] = None
        self.args.update(args)

        if self.args['SendMail'] :
            if self.args['SendMail'].lower() == "true" :
                self.args['SendMail'] = True

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
        msg += " => SendMail: %s\n" % self.args['SendMail']
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

        if event.startswith("AdminControl:ForwardTo:"):
            #  //
            # // Message recieved to be forwarded to the Bot
            #//  named in the payload
            self.forwardToBot(event, payload)
            return
            

        return


    def forwardToBot(self, message, payload):
        """
        _forwardToBot_

        A message has been recieved to be forwarded to a Bot.

        Determine which Bot it should go to, then pass it on

        """
        parseMsg = message.split("AdminControl:ForwardTo:")
        if len(parseMsg) < 2:
            msg = "Unable to parse message and forward to Bot:\n"
            msg += message
            logging.error(msg)
            return
        botName = parseMsg[1]
        if not self.bots.has_key(botName):
            msg = "Recieved Message for Bot: %s\n" % botName
            msg += "But there are no Bots with that name:\n"
            msg += str(self.bots.keys())
            logging.error(msg)
            return
        try:
            self.bots[botName].handleForwardedMessage(payload)
        except Exception, ex:
            msg = "Error forwarding message:\n"
            msg += " %s\n " % message
            msg += " With Payload:\n %s\n" % payload
            msg += " To Bot: %s\n" % botName
            msg += str(ex)
            logging.error(msg)
            
            
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

        self.ms.publishUnique("AdminControl:BotCycle", "",
                              self.args['BotPeriod'])
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
            bot.args.update(self.args)
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

        # forward messages to Bots
        for botName in self.botList:
            messageName = "AdminControl:ForwardTo:%s" % botName
            self.ms.subscribeTo(messageName)

        self.startBots()
        
        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("AdminControl: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()
            
            

