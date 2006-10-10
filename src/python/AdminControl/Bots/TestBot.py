#!/usr/bin/env python
"""
_TestBot_

Example/Test Bot plugin for AdminControl Automated Bot system

"""

import logging

from AdminControl.Bots.BotInterface import BotInterface
from AdminControl.Registry import registerBot



class TestBot(BotInterface):

    def __init__(self):
        self.skipCycles = 2


    def __call__(self):
        logging.info("TestBot Invoked!!!!")
        return


registerBot(TestBot, TestBot.__name__)


        
    









