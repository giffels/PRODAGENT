#!/usr/bin/env python
"""
Unittest MessageService.MessageService module
"""


import unittest
import random

from MessageService.MessageService import MessageService

class MessageServiceUnitTests(unittest.TestCase):
    """
    TestCase for MessageService module 
    """

    def setUp(self):
        print "**************NOTE MessageService_t***********"
        print "Make sure the test input does not conflict"
        print "with the data in the database!"
        print " "
        print "Make sure the database (and client) are properly"
        print "configured."
        print " "
        print "This test might take a few seconds (sending thousands"
        print "of messages). "
        print " "
        
        self.messageService={}
        for i in xrange(0,100):
            self.messageService[i]=MessageService()
            self.messageService[i].registerAs("Component"+str(i))
            
    def testA(self):
        for i in xrange(0,100):
            self.messageService[i].subscribeTo("Event4Component"+str(i))

        for i in xrange(0,1000):
           j=random.randint(0,99)
           k=random.randint(0,99)
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j))
           self.messageService[k].rollback()
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j))
           self.messageService[k].commit()
           (type,payload)=self.messageService[j].get()
           self.assertEqual(type,"Event4Component"+str(j))
           self.messageService[j].commit()
           self.assertEqual(payload,"someEventFrom"+str(k)+"4Component"+str(j))
            

    def runTest(self):
        self.testA()
            
if __name__ == '__main__':
    unittest.main()
