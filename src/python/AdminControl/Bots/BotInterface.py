#!/usr/bin/env python
"""
_BotInterface_

Base class for all Bots that get peroiodically invoked by the AdminControl
Periodic Cycle

"""

import logging

class BotInterface:
    """
    _BotInterface_

    Base class for all Bots

    """
    def __init__(self):
        self.cycleCount = 0
        self.skipCycles = 1
        self.active = True
        

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
                msg = "Error invoking Bot: %s\n" % self.__class__.__name__)
                msg += str(ex)
                logging.error(ex)
            self.cycleCount = 0
        return


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
    
    
        
