#!/usr/bin/env python
"""
_ServerTest_


"""
import os
import socket
import unittest
import threading
import time
import xmlrpclib

from MessageService.MessageService import MessageService


class ComponentServerTest(unittest.TestCase):
    """
    TestCase implementation for ServerTest
    """
    def setUp(self):
        print "**************NOTE ComponentServerTest***********"
        print " Make sure ONLY the job cleanup component is running!"
        print " "

        # we use this for event publication.
        self.ms=MessageService()
        self.ms.registerAs("TestComponent")

    def testA(self):
        """publish events to turn logging on"""
        try:
            self.ms.publish("JobCleanup:StartDebug", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed calling ms.publish:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """publish cleanup events"""
        try:
            self.ms.publish("JobCleanup", "none")
            self.ms.commit()
        except StandardError, ex:
            msg = "Failed calling ms.publish:\n"
            msg += str(ex)
            self.fail(msg)
         

    def runTest(self):
         self.testA()
         self.testB()

if __name__ == '__main__':
    unittest.main()

    
