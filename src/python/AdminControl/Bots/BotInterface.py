#!/usr/bin/env python
"""
_BotInterface_

Base class for all Bots that get peroiodically invoked by the AdminControl
Periodic Cycle

"""

import logging
import os

class BotInterface:
    """
    _BotInterface_

    Base class for all Bots

    """
    def __init__(self):
        self.cycleCount = 0
        self.skipCycles = 1
        self.active = True
        self.args = {}


    def handleForwardedMessage(self, payload):
        """
        _handleForwardedMessage_

        Bots get subscribed to an event called
        AdminControl:ForwardTo:<BotName>
        allowing them to recieve async message payloads via the
        message service.

        The Payloads get forwarded to this method, which you can
        override to define how the Bot handles this call.

        """
        logging.debug("BotInterface.handleForwardedMessage(%s)" % payload)
        return
        

    def run(self):
        """
        _run_

        method that is called every time the AdminControl Cycle occurs.
        Increments the cycleCount, and compares it to the skipCycles
        count. When the cycleCount reaches skipCycles, the counter is reset
        and the call method of this Bot is invoked to perform its required
        actions

        """
        if not self.active:
            logging.debug("Bot: %s Not Active" % self.__class__.__name__)
            return
        self.cycleCount += 1
        if self.skipCycles == self.cycleCount:
            logging.info("Invoking Bot: %s " % self.__class__.__name__)
            try:
                self.__call__()
                logging.info("Bot %s Completed" % self.__class__.__name__)
            except Exception, ex:
                msg = "Error invoking Bot: %s\n" % self.__class__.__name__
                msg += str(ex)
                logging.error(ex)
            self.cycleCount = 0
        return


    def mail(self, message):
        """
        _mail_
        
        Method that sends an email notification to the 'MailTo' field in 
        case any of the bots had taken an action. This method has to be 
        explicitly called.

        """
        if not self.args['SendMail'] :
            logging.info("SendMail flag is False, not sending mail.")
            return

        if self.args['MailTo'] == None :
            mailTo = os.environ['USER']
        elif self.args['MailTo'].lower() == "none" :
            mailTo = os.environ['USER']
        else :
            mailTo = self.args['MailTo']

        host = os.environ['HOST']
        messageFileName = os.path.join(self.args['ComponentDir'], 'mail.txt')
        messageFile = open(messageFileName, 'w')
        messageFile.write(message)

        command = "mail -s '%s: ProdAgent. AdminControl Component Notification'" % (
            host)
        command += " %s" % (mailTo)
        command += " < %s" % (messageFileName)

        logging.info("\nSending notification mail.\nMailTo: %s\n" % mailTo)
        logging.info("Command: %s" % command)
        messageFile.close()
        os.system(command)


    def __call__(self):
        """
        _operator()_

        Override this call to perform the periodic action
        for the Bot Implementation
        """
        msg = "BotInterface.__call__ Not Implemented for class %s" % (
            self.__class__.__name__,
            )
        raise NotImplementedError, msg
    
    
        
