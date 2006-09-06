#!/usr/bin/env python
"""
Unittest ProdMgr.ProdMgr module
"""


import unittest

import ProdMgrInterface.Interface as ProdMgrAPI

class ProdMgrUnitTests(unittest.TestCase):
    """
    TestCase for ProdMgr module 
    """

    def setUp(self):
        print('set up')
        self.serverURL="https://stein.ultralight.org:8444/"
        self.request_id='myRequest'
    
    def testA(self):
        x=raw_input("This rest requires a prodmgr exposes as a service and have the "+\
           " prodAgentClient.py and  schedulerClient.py web service unit test run to prepare "+\
           " the prodmgr state to deal with the prodagent input (press any key to continue)")

        print('testA')
        result=ProdMgrAPI.userID(self.serverURL) 

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(result,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(result,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'userID')
        self.assertEqual(result,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()

        print('ProdAgent ID is :'+str(result))

    def testB(self):
        print('testB')
        allocations=ProdMgrAPI.acquireAllocation(self.serverURL,self.request_id,15)

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(allocations,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(allocations,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'acquireAllocation')
        self.assertEqual(allocations,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()

        print('Acquired : '+str(len(allocations))+' allocations: '+str(allocations))
        print('Releasing: '+str(allocations[0]))
        result=ProdMgrAPI.releaseAllocation(self.serverURL,allocations[0])

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(result,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(result,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'releaseAllocation')
        self.assertEqual(result,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()
        new_allocation=ProdMgrAPI.acquireAllocation(self.serverURL,self.request_id,1)

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(new_allocation,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(new_allocation,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'acquireAllocation')
        self.assertEqual(new_allocation,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()
        print('Acquired : '+str(new_allocation))

        

    def testC(self):
        print('testC')
        parameters={'numberOfJobs':20,
                    'prefix':'Wave1'}
        jobs=ProdMgrAPI.acquireJob(self.serverURL,self.request_id,parameters)

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(jobs,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(jobs,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'acquireJob')
        self.assertEqual(jobs,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()
        ProdMgrUnitTests.jobs=jobs
        print('Acquired : '+str(len(jobs))+' jobs: '+str(jobs))
        print('Start downloading the job specs:')
        for job in jobs:
            targetDir='/tmp'
            targetFile=job['URL'].split('/')[-1]
            print('Downloading: '+job['URL'])
            ProdMgrAPI.retrieveFile(job['URL'],targetDir+'/'+targetFile)
            print('Downloaded: '+targetFile)
        print('Downloaded job spec:')

    def testD(self):
        print('testD')
        jobs=ProdMgrUnitTests.jobs
        #pretend we are processing the jobs and release some
        for job_index in xrange(0,len(jobs)-5):
            jobspec=jobs[job_index]['jobSpecId']
            finished=ProdMgrAPI.releaseJob(self.serverURL,str(jobspec),30)

            print("Pretending that the client side is crashing!!")
            retrievedResult=ProdMgrAPI.retrieve()
            self.assertEqual(finished,retrievedResult[0])
            retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
            self.assertEqual(finished,retrievedResult[0])
            retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'releaseJob')
            self.assertEqual(finished,retrievedResult[0])
            print("Recovered data lost in last service call")

            ProdMgrAPI.commit()

    def testE(self):
        print('testE')
        #acquire some jobs for this request:
        parameters={'numberOfJobs':20,
                    'prefix':'Wave2'}
        jobs=ProdMgrAPI.acquireJob(self.serverURL,self.request_id,parameters)

        print("Pretending that the client side is crashing!!")
        retrievedResult=ProdMgrAPI.retrieve()
        self.assertEqual(jobs,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
        self.assertEqual(jobs,retrievedResult[0])
        retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'acquireJob')
        self.assertEqual(jobs,retrievedResult[0])
        print("Recovered data lost in last service call")

        ProdMgrAPI.commit()
        print('Acquired : '+str(len(jobs))+' jobs: '+str(jobs))
        ProdMgrUnitTests.jobs+=jobs

    def testF(self):
        print('testF')
        jobs=ProdMgrUnitTests.jobs
        for job_index in xrange(0,len(jobs)):
           jobspec=jobs[job_index]['jobSpecId']
           events_completed=jobs[job_index]['end_event']-jobs[job_index]['start_event']+1
           try:
              finished=ProdMgrAPI.releaseJob(self.serverURL,str(jobspec),events_completed)

              print("Pretending that the client side is crashing!!")
              retrievedResult=ProdMgrAPI.retrieve()
              self.assertEqual(finished,retrievedResult[0])
              retrievedResult=ProdMgrAPI.retrieve(self.serverURL)
              self.assertEqual(finished,retrievedResult[0])
              retrievedResult=ProdMgrAPI.retrieve(self.serverURL,'releaseJob')
              self.assertEqual(finished,retrievedResult[0])
              print("Recovered data lost in last service call")

              ProdMgrAPI.commit()
              if type(finished)==bool:
                 if finished:
                    print('finished request, killing remaining jobs '+str(len(jobs)-job_index))
                    break
        # we should get some errors as we releasing the same job twice
           except Exception,ex:
               print(str(ex))
               print("INTENTIONAL ERROR!!! ")

    def runTest(self):
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        self.testE()
        self.testF()
            
if __name__ == '__main__':
    unittest.main()
