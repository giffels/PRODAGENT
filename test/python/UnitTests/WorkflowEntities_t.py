#!/usr/bin/env python

"""
Unittest WorkflowEntities module
"""


import logging
import time
import unittest

from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import Job
from ProdAgent.WorkflowEntities import File
from ProdAgent.WorkflowEntities import Workflow
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session

class WorkflowEntitiesUnitTests(unittest.TestCase):
    """
    TestCase for WorkflowEntities module 
    """

    def setUp(self):
#        logging.getLogger().setLevel(logging.DEBUG)
        logging.disable(logging.ERROR)
        self.maxFlow = 10
        # self.maxFlow*self.maxAlloc allocations
        self.maxAlloc = 10
        # self.maxFlow*self.maxAlloc*self.maxJob jobs (don't go lower than 100)
        self.maxJob = 10

    def testA(self):
        print('Inserting workflows')
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            # register assigned workflowspecs
            for i in xrange(0,self.maxFlow):
                parameters={}
                parameters['priority']=i
                parameters['prod_mgr_url']='http://somewhere.com'
                parameters['request_type']='event'
                parameters['owner']='componentX'
                workflowID='aWorkflow'+str(i)
                Workflow.register(workflowID,parameters)
            #  register some that we are going to delete again
            for i in xrange(0,self.maxFlow):
                parameters={}
                parameters['priority']=i
                parameters['prod_mgr_url']='http://somewhere.com'
                parameters['request_type']='event'
                workflowID='workflow2BDeleted'+str(i)
                Workflow.register(workflowID,parameters)
            # insert the same workflowspecs again (should not give an error).
            for i in xrange(0,self.maxFlow):
                parameters={}
                parameters['priority']=i
                parameters['prod_mgr_url']='http://somewhere.com'
                parameters['request_type']='event'
                workflowID='aWorkflow'+str(i)
                Workflow.register(workflowID,parameters)
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed TestA:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        print('Deleting workflows')
        try:
            # deleting some workflows.
            Session.connect()
            Session.start_transaction()
            IDs=[]
            for i in xrange(0,self.maxFlow):
                workflowID='workflow2BDeleted'+str(i)
                IDs.append(workflowID)
                Workflow.remove(IDs)
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed TestB:\n"
            msg += str(ex)
            self.fail(msg)
   
    def testC(self):
        print('Adding workflow file location')
        try:
            Session.connect()
            Session.start_transaction()
            workflows=Workflow.getNotDownloaded()
            workflows_check=workflows
            # download workflow spec of assigned workflows.
            for workflow in workflows:
                print('Downloading: '+workflow['id']+' from '+workflow['prod_mgr_url'])
                Workflow.setWorkflowLocation(workflow['id'],'some/location.xml')
            Session.commit_all()
            self.assertEqual([],Workflow.getNotDownloaded())
            for workflow in workflows_check:
                self.assertEqual(True,Workflow.exists(workflow['id']))
            for workflow in workflows_check:
                self.assertEqual(False,Workflow.isDone(workflow['id']))
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed TestC:\n"
            msg += str(ex)
            self.fail(msg)

    def testD(self):
        # create message service instance
        self.ms = MessageService()
        # register
        self.ms.registerAs("Test")
        File.ms=self.ms


        print('Add allocations')
        Session.connect()
        Session.start_transaction()
        amount=Workflow.amount()
        print('The prodagent is working on '+str(amount)+' requests')
        all_allocations=[]
        # register allocations associated to workflows.
        for i in xrange(0,amount):
            workflow=Workflow.getHighestPriority(i)
            print('Priority and name of '+str(i)+'th request is:'+workflow['id'])
            allocations=[]
            for j in xrange(0,self.maxAlloc):
                parameters={}
                parameters['id']=workflow['id']+'_allocation'+str(j)
                all_allocations.append(parameters['id'])
                parameters['prod_mgr_url']='http://somewhere'
                parameters['details']={'var1':'val1','var2':'val2'}
                parameters['events_allocated'] = 10
                allocations.append(parameters)
            Allocation.register(workflow['id'],allocations)
        # register jobs from allocations (job cutting)
        print('add jobs')
        job_count=0
        for allocation in all_allocations:
             for j in xrange(0,self.maxJob):
                 job_count+=1
                 job={}
                 jobID=allocation+'_'+str(j)
                 job['id']=allocation+'_'+str(j)
                 job['spec']='/some/where/on/the/disk'
                 job['job_type']='event'
                 job['owner']='a owner'
                 Job.register(None,allocation,job)
        print('register again (should only update)')
        all_jobs=[]
        for allocation in all_allocations:
             for j in xrange(0,self.maxJob):
                 job_count+=1
                 job={}
                 jobID=allocation+'_'+str(j)
                 all_jobs.append(jobID)
                 job['id']=allocation+'_'+str(j)
                 job['spec']='/some/else/where/on/the/disk'
                 job['max_retries']=10
                 job['max_racers']=1
                 Job.register(None,allocation,job)
 
        Session.commit_all()
        print('check if parameters got registered correctly')
        jobs = Job.get(all_jobs[:10])
        for job in jobs:
            self.assertEqual(10,job['max_retries'])
            self.assertEqual(1,job['max_racers'])
        job=Job.get(all_jobs[11])
        self.assertEqual(10,job['max_retries'])
        self.assertEqual(1,job['max_racers'])

        print('check if parameters got registered correctly')
        jobs=Job.getRange(12,10)
        for job in jobs:
            self.assertEqual(10,job['max_retries'])
            self.assertEqual(1,job['max_racers'])
        Session.commit_all()
        Session.close_all()

        print('remove some jobs and see if they exists or not.')
        Session.connect()
        Session.start_transaction()
        Job.remove(all_jobs[0:10])
        for job in all_jobs[0:10]:
            self.assertEqual(False,Job.exists(job))
        for job in all_jobs[10:]:
            self.assertEqual(True,Job.exists(job))

        print('insert 10 new jobs to replace the deleted ones')
        # these jobs depend only from a workflow (no allocations)
        workflow=Workflow.getHighestPriority(1)
        for i in xrange(0,10):
            job={}
            job['id']=workflow['id']+'_'+str(i)
            all_jobs[i]=job['id']
            job['spec']='/some/else/where/on/the/disk'
            job['max_retries']=10
            job['max_racers']=1
            Job.register(workflow['id'],None,job)

        print('set the cache dir. ')
        for job in all_jobs[10:]:
            Job.setCacheDir(str(job),'/this/is/a/cachedir')
        Job.setMaxRacers(all_jobs[10:],2)
        print('do some exception checking')
        try:
           Job.setMaxRacers(all_jobs[10],0)
        except ProdException,ex:
           self.assertEqual(ex['ErrorNr'],3005)

        print('set maxretries')
        Job.setMaxRetries(all_jobs[10:],2)
        try:
           Job.setMaxRetries(all_jobs[10],0)
        except ProdException,ex:
           self.assertEqual(ex['ErrorNr'],3006)
        jobs=Job.get(all_jobs[10:])
        for job in jobs[11:]:
            self.assertEqual(job['max_retries'],2)
            self.assertEqual(job['max_racers'],2)
        Session.commit_all()
        print('set some parameters')
        Job.setMaxRetries(all_jobs[10],4)
        Job.setMaxRacers(all_jobs[10],4)
        job=Job.get(all_jobs[10])
        self.assertEqual(job['max_retries'],4)
        self.assertEqual(job['max_racers'],4)
        Session.commit_all()
        # test exceptions for jobstates:
        for job in xrange(0,10):
            job_id='non_existing_id'
            try:
                Job.setState(job,'create')
            except ProdException,ex:
                self.assertEqual(ex['ErrorNr'],3009)
            try:
                Job.setState(job,'inProgress')
            except ProdException,ex:
                self.assertEqual(ex['ErrorNr'],3009)
            try:
                Job.setState(job,'submit')
            except ProdException,ex:
                self.assertEqual(ex['ErrorNr'],3009)
            try:
                Job.setState(job,'finished')
            except ProdException,ex:
                self.assertEqual(ex['ErrorNr'],3009)

        print('test some more exceptions of jobstates')
        bulkJobs = all_jobs[0:100]
        Job.setState(bulkJobs,'released')
        Job.setState(bulkJobs,'create')
        Job.setState(bulkJobs,'inProgress')

        for job in all_jobs[100:]:
            Job.setState(job,'released')
            Job.getByState(['released','created'])
            Job.getByState('released')
            Job.getByState()
            Job.setState(job,'create')
            Job.setState(job,'create')
            Job.setState(job,'inProgress')
            # use non defined state
            try:
                Job.setState(job,'blob')
                assert 1 == 2
            except Exception,ex:
                assert 1 == 1
            Session.commit_all()

        print('let some jobs succeed')
        for job in all_jobs[:100]: 
            Job.setState(job,'submit')
            Job.setState(job,'finished')
            Job.setEventsProcessedIncrement(job,10)
        Session.commit_all()
        all_fileList=[]
        for job in all_jobs[10:100]: 
            fileList=[]
            for file in xrange(0,10):
                fileInfo={}
                fileInfo['lfn']='/somewhere/'+str(job)+'/'+str(file)+'/over/the/rainbow'
                fileInfo['events']=file*30
                fileList.append(fileInfo)
                all_fileList.append(fileInfo['lfn'])
            File.register(job,fileList)

        print('register some merge successes and failures') 
        File.merged(all_fileList[0])
        File.merged(all_fileList[1:298])
        File.merged(all_fileList[298],failed=True)
        File.merged(all_fileList[299:],failed=True)
        print('register some files that are not there')
        File.merged('does_not_exist_id')
        File.merged('does_not_exist_id',failed=True)
        # now check if the jobs we submitted are really finished.
        print('let some other jobs fail')
        for job in all_jobs[100:]:
            Job.setState(job,'submit')
            Job.setState(job,'finished')
        Session.commit_all()
        Session.close_all()
        
    def runTest(self):
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        
            
if __name__ == '__main__':
    unittest.main()
