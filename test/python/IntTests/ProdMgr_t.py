#!/usr/bin/env python
"""
Unittest ProdMgr.ProdMgr module
"""

import random
import unittest

from MessageService.MessageService import MessageService

class ProdMgrUnitTests(unittest.TestCase):
    """
    TestCase for ProdMgr module which tests the component
    which involves interaction with the ProdMgr
    """

    def setUp(self):
        # we use this for event publication.
        self.ms=MessageService()
        self.ms.registerAs("TestComponent")
        self.ms.subscribeTo("CreateJob")
        self.requests=5
        self.prodMgrUrl='https://localhost:8444/'
        # subscribe on the events this test produces
        # so we can verify this in the database

    
    def testA(self):
        msg="""
  These tests requires a running prodMgr and the urls 
  used in this test need to be consistent with the urls 
  where the different prodMgr(s) are running. In order 
  to successfully run this test you first need to run test 
  ProdMgrInterfactPrepare_t.py of the prodMgr as this 
  will create the necessary requests.

   >>>Make sure the ProdMgrInterface component is running<<<!

   Press any key to continue
         """ 
        raw_input(msg)
        try:
            print("--->sending debug events")
            self.ms.publish("ProdMgrInterface:StartDebug", "none")
            for i in xrange(0,self.requests):
               priority=random.randint(0,10)
               self.ms.publish("ProdMgrInterface:AddRequest", self.prodMgrUrl+"?RequestId=requestID_"+str(i)+"?Priority="+str(priority))
            self.ms.publish("ResourcesAvailable", str(15))
            self.ms.commit()
          
            # check if some jobs where created 
            for i in xrange(0,15): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                self.ms.commit()

            # again publish resources available.
            self.ms.publish("ResourcesAvailable", str(30))
            self.ms.commit()
            for i in xrange(0,30): 
                type, payload = self.ms.get()
                print("Message type: "+str(type)+", payload: "+str(payload))
                self.ms.commit()
            #self.ms.publish("JobSuccess", "ASuccessJobSpecID")
            #self.ms.publish("GeneralJobFailure", "AFailedJobSpecID")

        except StandardError, ex:
            msg = "Failed testA\n"
            msg += str(ex)
            self.fail(msg)


    def runTest(self):
        self.testA()
            
if __name__ == '__main__':
    unittest.main()
