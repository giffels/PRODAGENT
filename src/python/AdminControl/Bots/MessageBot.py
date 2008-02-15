#!/usr/bin/env python
"""
_MessageBot_

Periodic tests and cleanup of the MessageService

"""

import logging

from AdminControl.Bots.BotInterface import BotInterface
from AdminControl.Registry import registerBot

from MessageService.MessageService import MessageService



class MessageBot(BotInterface):
    """
    _MessageBot_

    Check MessageService health, clean out old messages from history.
    Generate Alerts for large message queues for a component, or
    outstanding messages etc.

    """
    def __init__(self):
        BotInterface.__init__(self)
        self.skipCycles = 1


    def __call__(self):
        """
        _operator()_

        Invoke Bot

        """
        logging.info("MessageBot Invoked")
        msgSvc = MessageService()
        msgSvc.registerAs("MessageBot")
        

        #  //
        # // Clean Message History Older than 3 days
        #//
        msgSvc.cleanHistory("72:00:00")
        

        return

    
registerBot(MessageBot, MessageBot.__name__)
