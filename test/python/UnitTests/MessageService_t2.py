#!/usr/bin/env python
"""
Unittest MessageService.MessageService module
"""

import unittest
import random
import time

from MessageService.MessageService import MessageService
import logging
from logging.handlers import RotatingFileHandler

class MessageServiceUnitTests(unittest.TestCase):
    """
    TestCase for MessageService module 
    """

    def setUp(self):

        print """
                 Message Service test with connections closed and
                 forced refreshed connections inside transactions.
                 See logfile for more details"""

        # define logging
        # create log handler
        logHandler = RotatingFileHandler("logfile","a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.DEBUG)

        # create message service instance
        self.ms = MessageService()
         
        # create components
        self.ms.registerAs("Component1")
        self.ms.registerAs("Component2")

        # subscribe Component2 to messages of type MessageType1
        self.ms.subscribeTo("MessageType1")

    def testA(self):

        # purge messages
        print "Purging messages"
        self.ms.purgeMessages()

        # Component1 sends 10 messages
        self.ms.registerAs("Component1")
        print "Component1 sends messages: ",
        for index in range(10):
            self.ms.publish("MessageType1",str(index))
            print index,
        print ""
        self.ms.commit()

        # Component2 gets them
        self.ms.registerAs("Component2")
        print "Component2 gets: ",
        for index in range(10):
            type, payload = self.ms.get(wait = False)
            print payload,
        print ""
        self.ms.commit()

        # Close connection inside a transaction
        
        print "Close connection inside a transaction"
        self.ms.registerAs("Component1")
        print "Sending first message"
        self.ms.publish("MessageType1","11")
        print "Closing connection!"
        self.ms.conn.close()
        print "Sending second message"
        self.ms.publish("MessageType1","12")
        self.ms.commit()

        # Component2 should get both
        self.ms.registerAs("Component2")
        print "Component2 gets: ",
        for index in range(2):
            type, payload = self.ms.get(wait = False)
            print payload,
        print ""
        self.ms.commit()

        print "Transaction was recovered!"

        # Force refresh
        self.ms.refreshPeriod = 0
        print "Force a refresh event"

        self.ms.registerAs("Component1")
        print "Sending first message"
        self.ms.publish("MessageType1","14")
        print "Sending second message"
        self.ms.publish("MessageType1","15")
        print "Committing"
        self.ms.commit()
        print "Sending third message"
        self.ms.publish("MessageType1","16")
        print "Committing"
        self.ms.commit()

        # Component2 should get all three
        self.ms.registerAs("Component2")
        print "Component2 gets: ",
        for index in range(3):
            type, payload = self.ms.get(wait = False)
            print payload,
        print ""
        self.ms.commit()

if __name__ == '__main__':
    unittest.main()
