#!/usr/bin/env python
"""
Unittest MessageService.MessageService module
"""


import unittest
import random
import time

from MessageService.MessageService import MessageService

class MessageServiceUnitTests(unittest.TestCase):
    """
    TestCase for MessageService module 
    """

    def setUp(self):
        self.messageServices=20
        self.messages=1000
        self.delay=5
        self.delay_format='00:00:05'
    
    def testA(self):
        print "**************NOTE MessageService_t***********"
        print "Make sure the test input does not conflict"
        print "with the data in the database!"
        print " "
        print "Make sure the database (and client) are properly"
        print "configured."
        print " "
        print "This test might take a few seconds (sending thousands"
        print "of messages). "
        print "********************************************** "
        MessageServiceUnitTests.messageService={}
        print('testA: preparing registration')
        for i in xrange(0,self.messageServices):
            print('registering component '+str(i))
            self.messageService[i]=MessageService()
            self.messageService[i].registerAs("Component"+str(i))

        print('testA: preparing subscriptions')        
        for i in xrange(0,self.messageServices):
            self.messageService[i].subscribeTo("Event4Component"+str(i))
            self.messageService[i].subscribeTo("ShortEvent4Component"+str(i))

    def testB(self):
        print('*****************************')
        print('testB: messages with delay 0')
        start=time.time()

        # messages without delay
        for i in xrange(0,self.messages):
           j=random.randint(0,(int(self.messageServices)-1))
           k=random.randint(0,(int(self.messageServices)-1))
           MessageServiceUnitTests.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j))
           MessageServiceUnitTests.messageService[k].rollback()
           MessageServiceUnitTests.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j))
           MessageServiceUnitTests.messageService[k].commit()
           (type,payload)=MessageServiceUnitTests.messageService[j].get()
           self.assertEqual(type,"Event4Component"+str(j))
           MessageServiceUnitTests.messageService[j].commit()
           self.assertEqual(payload,"someEventFrom"+str(k)+"4Component"+str(j))
        print('testB took '+str(time.time()-start)+' seconds')    
        # purge messages for next test
        MessageServiceUnitTests.messageService[0].purgeMessages()

    def testC(self):
        print('*****************************')
        print('testC: messages with a delay between 0 and '+str(self.delay))

        start=time.time()
        message_receivers=[]
        message_senders=[]
        for i in xrange(0,self.messages):
           j=random.randint(0,(int(self.messageServices)-1))
           message_receivers.append(j)
           k=random.randint(0,(int(self.messageServices)-1))
           message_senders.append(k)
           delay_random=random.randint(2,self.delay)
           delay_format='00:00:0'+str(delay_random)
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j),delay_format)
           self.messageService[k].rollback()
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j),delay_format)
           self.messageService[k].commit()

        print('Sleeping for '+str(self.delay)+' seconds to make sure we get')
        print('the messages at the first try (so the message service does not')
        print('goes to sleep during a get operation)')
        time.sleep(self.delay+1)
   
        for p in xrange(0,len(message_receivers)):
           j=message_receivers[p]
           k=message_senders[p]
           (type,payload)=self.messageService[j].get()
           self.assertEqual(type,"Event4Component"+str(j))
           self.messageService[j].commit()
           self.assertEqual(payload,"someEventFrom"+str(k)+"4Component"+str(j))

        print('testC took '+str(time.time()-start)+' seconds')    
        # purge messages for next test
        self.messageService[0].purgeMessages()

    def testD(self):
        print('*****************************')
        print('testD: sending '+str(5*self.messages)+' with a long delay')
        print('After that sending '+str(self.messages)+ ' with delay 0 and retrieve these last ')
        print('messages to see what influence the delayed messages have on insertion and retrieval')
        for i in xrange(0,5*self.messages):
           j=random.randint(0,(int(self.messageServices)-1))
           k=random.randint(0,(int(self.messageServices)-1))
           # make the delay very long so we do not retrieve this message
           delay_format='1:00:00'
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j),delay_format)
           self.messageService[k].commit()

        message_receivers=[]
        message_senders=[]
        start=time.time()
        for i in xrange(0,self.messages):
           j=random.randint(0,(int(self.messageServices)-1))
           message_receivers.append(j)
           k=random.randint(0,(int(self.messageServices)-1))
           message_senders.append(k)
           delay_random=random.randint(2,self.delay)
           self.messageService[k].publish("ShortEvent4Component"+str(j),"someShortEventFrom"+str(k)+"4Component"+str(j))
           self.messageService[k].commit()

        
        for p in xrange(0,len(message_receivers)):
           j=message_receivers[p]
           k=message_senders[p]
           (type,payload)=self.messageService[j].get()
           self.assertEqual(type,"ShortEvent4Component"+str(j))
           self.messageService[j].commit()
           self.assertEqual(payload,"someShortEventFrom"+str(k)+"4Component"+str(j))
        print('testD took '+str(time.time()-start)+' seconds')
        print('(Discarding the '+str(5*self.messages)+' messages with a long delay)')    
        # purge messages for next test
        self.messageService[0].purgeMessages()
        
    def testE(self):
        print('*****************************')
        print('testE: messages with a delay between 0 and '+str(self.delay))
        print('this test might take a while if delay and messages is large!')
        print('By default the message service sleeps 5 seconds during every ')
        print('try which it does not in the other tests as messages are retrieved')
        print('immediately')

        start=time.time()
        for i in xrange(0,self.messages):
           j=random.randint(0,(int(self.messageServices)-1))
           k=random.randint(0,(int(self.messageServices)-1))
           delay_random=random.randint(2,self.delay)
           delay_format='00:00:0'+str(delay_random)
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j),delay_format)
           self.messageService[k].rollback()
           self.messageService[k].publish("Event4Component"+str(j),"someEventFrom"+str(k)+"4Component"+str(j),delay_format)
           self.messageService[k].commit()
           print('getting message for component '+str(j)+' from component '+str(k))
           (type,payload)=self.messageService[j].get()
           print('got message ')
           self.assertEqual(type,"Event4Component"+str(j))
           self.messageService[j].commit()
           self.assertEqual(payload,"someEventFrom"+str(k)+"4Component"+str(j))
        print('testE took '+str(time.time()-start)+' seconds')    
        # purge messages for next test
        self.messageService[0].purgeMessages()
      
    def runTest(self):
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        self.testE()
            
if __name__ == '__main__':
    unittest.main()
