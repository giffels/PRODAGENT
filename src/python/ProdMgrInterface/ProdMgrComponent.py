#!/usr/bin/env python
"""

_ProdMgrComponent_

Component that communicates with the ProdMgr to retrieve work
and report back details of completed jobs



"""

from logging.handlers import RotatingFileHandler

import os
import random
import logging
import time

from ProdCommon.Database import Session
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from JobQueue.JobQueueDB import JobQueueDB
from MessageService.MessageService import MessageService
from ProdAgentCore.Codes import errors
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger

from ProdMgrInterface.Registry import retrieveHandler
from ProdMgrInterface.Registry import Registry
from ProdMgrInterface import MessageQueue
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import Workflow
from ProdMgrInterface import State
from ProdMgrInterface import Interface as ProdMgr

import ProdMgrInterface.Interface as ProdMgrAPI

class ProdMgrComponent:
    """
    _ProdMgrComponent_

    Component that interacts with the ProdMgr

    """
    def __init__(self, **args):
    
       try:
            self.args = {}
            self.args['JobQueue'] =  JobQueueDB()
            self.args['Logfile'] = None
            self.args['JobInjection'] = 'direct'
            self.args['QueueLow'] = 10
            self.args['AllocationSize'] = 10
            self.args['QueueHigh'] = 30
            self.args['QueueInterval'] = '00:01:00'
            self.args['ParallelRequests'] = 1
            self.args['Locations'] = 'none'
            self.args['ProdMgrFeedback'] = 'delay'
            self.args.update(args)
            if self.args['Logfile'] == None:
               self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                    "ComponentLog")
            #  //
            # // Log Handler is a rotating file that rolls over when the
            #//  file hits 1MB size, 3 most recent files are kept
            logHandler = RotatingFileHandler(self.args['Logfile'],
                                             "a", 1000000, 3)
            logFormatter = logging.Formatter("%(asctime)s:%(module)s:%(message)s")
            logHandler.setFormatter(logFormatter)
            logging.getLogger().addHandler(logHandler)
            logging.getLogger().setLevel(logging.DEBUG)
            #  //
            # // Set up formatting for the logger and set the
            #//  logging level to info level
            if (int(self.args['QueueHigh']) < int(self.args['QueueLow'])) or \
                (int(self.args['QueueLow']) < 0) :
                logging.info("QueueHigh smaller than QueueLow")
                logging.info("or QueueLow is negative, using default settings")
                self.args['QueueLow'] = 10
                self.args['QueueHigh'] = 30
            logging.info("ProdMgrComponent Started...")
            logging.info("I am going to sleep for 30 seconds to give other components the")
            logging.info("chance to start up and subscribe to my messages, otherwise I might")
            logging.info("send messages before components have subscribed to them")
            logging.info("Start parameters: "+str(self.args))
            time.sleep(30)     
       except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        #  //
        # // Control Events for this component
        #//
        if event == "ProdMgrInterface:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "ProdMgrInterface:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        if event == "ProdMgrInterface:AddRequest":
            self.addRequest(payload)
            return

        #  //
        # // Actual work done by this component
        #//
        if event == "ProdMgrInterface:ResourcesAvailable":
            try:
                jobVal = int(payload)
            except ValueError:
                jobVal = 1
            self.retrieveWork(jobVal)
            return
        elif event =="ProdMgrInterface:SetLocations":
            logging.debug("Contacting prodmgrs to set locations")
            self.setLocations(payload)
        elif event =="ProdMgrInterface:AcquireRequests":
            logging.debug("Contacting prodmgrs to acquire new requests")
            self.acquireRequests(payload)
        elif event =="ProdMgrInterface:CheckQueue":
            logging.debug("Checking Queue")
            self.checkQueue(payload)
        elif event =="ProdMgrInterface:JobCutSize":
            logging.debug("Setting job cut size to: "+str(payload))
            self.args['JobCutSize']=int(payload)
            return
        elif event == "JobSuccess":
            self.reportJobSuccess(payload)
            return
        elif event == "ProdMgrInterface:JobSuccess":
            self.reportJobSuccessFinal(payload)
            return
        elif event == "GeneralJobFailure":
            self.reportJobFailure(payload)
            return
        elif event == "ProdMgrInterface:CleanWorkflow":
            self.cleanWorkflow(payload)
            return
        elif event == "ProdMgrInterface:IncreaseEvents":
            self.increaseEvents(payload)
            return


    def retrieveWork(self, numberOfJobs):
       """
       _retrieveWork_

       
       """
       # the algorithms works as follows:
       # check the last logged state. If it is start, check if there is anything 
       # left in the queue that failed when interacting with another application (prodmgr)
       # if not, proceed from the beginning, else proceed from the state that has
       # been logged.

       # we have a session for default 
       # and one for the states (called "ProdMgrInterface"), as we commit in between
       # logging calls and setting the explicit state

       # retrieve state (from database)
       # the state determines where we need to start.

       try:
           Session.connect("ProdMgrInterface")
           Session.set_session("ProdMgrInterface")
           componentStateInfo=State.get("ProdMgrInterface")      
           # if this is the first time create the start state
           if componentStateInfo=={}:
                logging.debug("ProdMgrInterface state creation")
                State.insert("ProdMgrInterface","start",{})
                componentStateInfo['state']="start"
                requestIndex=0
           if componentStateInfo.has_key('state'):
               if componentStateInfo['state']=='start':
                   componentStateInfo['parameters']={}
                   componentStateInfo['parameters']['jobCutSize']=self.args['JobCutSize']
                   componentStateInfo['parameters']['numberOfJobs']=numberOfJobs 
                   componentStateInfo['parameters']['requestIndex']=0
                   componentStateInfo['parameters']['queueIndex']=0
                   componentStateInfo['parameters']['stateType']='normal'
                   componentStateInfo['parameters']['jobSpecDir']=self.args['JobSpecDir']
                   State.setParameters("ProdMgrInterface",componentStateInfo['parameters'])
           logging.debug("ProdMgrInterface state is: "+str(componentStateInfo['state']))
           # go to first state that is needed for the retrieveWork handling event
           componentState=componentStateInfo['state']
           if componentStateInfo['state']=='start':
               componentState="AcquireRequest"
           Session.commit()
           componentStateInfo=State.get("ProdMgrInterface")      
           while componentState!='start':
               state=retrieveHandler(componentState)
               componentState=state.execute()
           logging.debug("retrieveWork event handled")
       except Exception,ex:
           logging.debug("ERROR "+str(ex))              

    def addRequest(self, requestURL):
        """
        _addRequest_

        URL points to ProdMgr containing the requests, it should contain
        2 arguments:
        RequestID: The id of the request in the ProdMgr
        Priority: The priority value for that request

        Eg:
        https://cmsprodmgr.somehost.com?RequestID=1234&Priority=5
 
        """
        #  //
        # // Parse the URL. 
        #//
        try:
            components=requestURL.split('?')
            prodMgr=components[0]
            requestId=components[1].split("=")[1]
            priority=components[2].split("=")[1]
            request_type=components[3].split("=")[1]
            logging.debug("Add request: "+requestId+" with priority "+priority+" for prodmgr: "+prodMgr)
            parameters={'priority':priority,'request_type':request_type,'prod_mgr_url':prodMgr,'owner':'ProdMgrInterface'}
#AF : default for new request
            parameters['workflow_spec_file']='not_downloaded'
#AF
            Workflow.register(requestID,parameters) 
            logging.debug("Added request. There are now "+str(Workflow.amount())+" requests in the queue ")
        except Exception,ex:
            logging.debug("ERROR "+str(ex))

    def reportJobSuccess(self, frameworkJobReport):
        """
        _reportJobSuccess_

        Read the report provided and report the details back to the
        ProdMgr
        
        """
        logging.debug("Reporting Job Success "+frameworkJobReport)
        queued_events=retrieveHandler('QueuedMessages')
        queued_events.execute()
        Session.commit()
        state=retrieveHandler('ReportJobSuccess')
        stateParameters={}
        stateParameters['stateType']='normal'
        stateParameters['jobType']='success'
        stateParameters['prodMgrFeedback']=self.args['ProdMgrFeedback']
        stateParameters['jobReport']=frameworkJobReport
        state.execute(stateParameters)
        logging.debug("reportJobSuccess event handled")
        Session.set_session("default")

    def reportJobFailure(self, frameworkJobReport):
        """
        _reportJobFailure_

        Read the report provided and report the details back to the ProdMgr

        """
        logging.debug("Reporting Job Failure "+frameworkJobReport)

        queued_events=retrieveHandler('QueuedMessages')
        queued_events.execute()
        Session.commit()
        state=retrieveHandler('ReportJobSuccess')
        stateParameters={}
        stateParameters['stateType']='normal'
        stateParameters['jobType']='failure'
        stateParameters['prodMgrFeedback']=self.args['ProdMgrFeedback']
        stateParameters['jobReport']=frameworkJobReport
        state.execute(stateParameters)
        logging.debug("reportJobFailure event handled")
        Session.set_session("default")

    def reportJobSuccessFinal(self,jobID):
        logging.debug("Reporting ProdMgrInterface:JobSuccess for"+str(jobID))
        queued_events=retrieveHandler('QueuedMessages')
        queued_events.execute()
        Session.commit()
        state=retrieveHandler('ReportJobSuccessFinal')
        stateParameters={}
        stateParameters['id']=jobID
        state.execute(stateParameters)
        logging.debug("ProdMgrInterface:JobSuccess event handled")

    def setLocations(self,payload):
        # check if payload contains user defined location
        # if not use the ones defined in the configuration file
        if payload=='':
           if self.args['Locations']=='':
               return
           locations=self.args['Locations'].split(',')
        else:
           locations=payload.split(',')
        # retrieve prodmgrs this prodagent is associated to
        prodmgrs=self.args['ProdMgrs'].split(',')
        retry=False
        for prodmgr in prodmgrs:
            try:
                ProdMgr.setLocations(prodmgr,locations)
                ProdMgr.commit()
            except Exception,ex:
                retry=True
                logging.debug("WARNING: Could not set locations for "\
                   +prodmgr+"  "+str(ex))
        if retry: 
            logging.debug("Setting locations was not successful. I will try "+\
                "again later (HH:MM:SS) 00:10:00 ")
            self.ms.publish("ProdMgrInterface:SetLocations",payload,"00:05:00")

    def cleanWorkflow(self,payload):
        """
        _cleanWorkflow_

        Removes the workflow and notifies the prodmgrs that you do not want
        to be subscribe to this workflow.
        """
        workflow = Workflow.get(payload)
        if not workflow:
            logging.debug("Workflow already removed")
            return        
        try:
            ProdMgr.unsubscribeWorkflow(workflow['prod_mgr_url'], \
                self.args['AgentTag'], payload)
        except Exception,ex:
            msg = """
Unsubscribe for workflow: %s at prodmgr: %s unsuccessful: %s.
Retrying later.
            """ % (payload, workflow['prod_mgr_url'], str(ex))
            logging.debug(msg)
            self.ms.publish("ProdMgrInterface:CleanWorkflow",payload,"00:10:00")
        Workflow.remove(payload) 

    def increaseEvents(self,payload):
        """
        _increaseEvents_

        Increase events of a particular workflow at the prodmgr side.
        Usefule if a PA is working on a request and needs to increase it.
        """
        payloadParts = payload.split(',')
        workflow_id = payloadParts[0]
        eventIncrease = int(payloadParts[1])
        requestURL = Workflow.get(workflow_id)['prod_mgr_url']
        logging.info("Increasing request: "+str(workflow_id)+\
            " with "+str(eventIncrease)+ " events")
        ProdMgrAPI.increaseEvents(requestURL, workflow_id, eventIncrease)
        logging.info("Request increased")

    def acquireRequests(self,payload):
        
        interval=self.args['RetrievalInterval']
        # we do not need any robustness scenarios or queing of messages for this one.
        prodmgrs=self.args['ProdMgrs'].split(',')
        for prodmgr in prodmgrs:
            try:
                requests=ProdMgr.getRequests(prodmgr,self.args['AgentTag'])
                for request in requests['keep']:
                    parameters={'priority':request[1],'request_type':request[2],'prod_mgr_url':prodmgr}
                    parameters['owner']='ProdMgrInterface'
                    registered = Workflow.get(request[0])
                    if not registered:
                        logging.debug("Registering "+request[0])
#AF : default for new request
                        parameters['workflow_spec_file']='not_downloaded'
#AF
                        Workflow.register(request[0],parameters)
                    else:
                        logging.debug("Renewing registration of "+request[0])
                        Workflow.register(request[0],parameters, renew = True)
                logging.debug("Retrieved: "+str(len(requests['keep']))+' requests')
                ProdMgr.commit()
            except Exception,ex:
                logging.debug("WARNING: Could not retrieve requests for "\
                   +prodmgr+"  "+str(ex))
        # once we have the requests retrieve if necessary their workflow.
        # depending on the request we might already have the workflow.
        notDownloadedRequests=Workflow.getNotDownloaded()
        try:
            os.makedirs(self.args['WorkflowSpecDir'])
        except Exception,ex:
            logging.debug("WARNING: directory already exists "+str(ex))
        for request in notDownloadedRequests:
            logging.debug('Retrieving Workflow for '+str(request['id']))
            rest_result=ProdMgr.retrieveWorkflow(request['prod_mgr_url'],request['id'])
            workflowSpec = WorkflowSpec()
            workflowSpec.loadString(rest_result)
            workflowFileName=self.args['WorkflowSpecDir']+'/'+workflowSpec.workflowName()+'.xml'
            workflowSpec.save(workflowFileName)
            Workflow.setWorkflowLocation(request['id'],workflowFileName)
            #NOTE: we want to use the same session as for the workflowset call.
            self.ms.publish("NewDataset",workflowFileName) 
            self.ms.publish("NewWorkflow",workflowFileName) 
              
        if payload==self.args['RandomCheck']['AcquireRequests']:
            self.args['RandomCheck']['AcquireRequests'] =str(random.random())
            logging.debug("Contacting prodmgrs again in (HH:MM:SS): "+str(interval))
            self.ms.publish("ProdMgrInterface:AcquireRequests", \
                str(self.args['RandomCheck']['AcquireRequests']), interval)

    def checkQueue(self, payload):
       
        # we do not do anything if the queue is turned off.
        if self.args['JobInjection'] == 'direct':
            logging.debug('Queue use is not activated. Not doing anything')
            return
        interval = self.args['QueueInterval']
        logging.debug("Get job queue length")
        length = self.args['JobQueue'].queueLength(jobType = "Processing")
        logging.debug("Queue length is: "+str(length))

        if  length < int(self.args['QueueLow']):
            logging.debug('Queue length below minimum treshold. Acquiring work:')
            jobsNeeded = int(self.args['QueueHigh'])-length

            logging.debug("Checking if there are allocations with missing events")
            allocations = Allocation.hasMissingEvents()
            for allocation in allocations:
                logging.debug("Missing events found for: "+str(allocation['id']))
                allocation['details']['end_event'] = allocation['details']['start_event']+ allocation['events_missed'] - 1
                logging.debug("Prepare state for job submission")
                stateParameters = {}
                stateParameters['jobSpecId'] = allocation['id']
                stateParameters['jobCutSize'] = self.args['JobCutSize']
                stateParameters['RequestType'] = 'event'
                jobSubmission = retrieveHandler('JobSubmission')
                jobs = jobSubmission.jobCutAccounting(stateParameters, allocation)
                logging.debug('Jobs submitted, resetting missed events')
                Allocation.setEventsMissed(allocation['id'], 0)
                jobsNeeded = jobsNeeded - len(jobs)
                logging.debug(str(jobsNeeded)+' jobs needed')
                if jobsNeeded <= 0:
                    logging.debug('Reached maximum jobs needed. Breaking')
                    break

            allocs = jobsNeeded / int(self.args['AllocationSize'])
            rest = jobsNeeded-allocs*int(self.args['AllocationSize'])


            for i in xrange(0,allocs):
                logging.debug('Emitting ProdMgr:ResourcesAvailable with '+\
                    str(self.args['AllocationSize'])+' Jobs') 
                self.ms.publish("ProdMgrInterface:ResourcesAvailable",\
                    str(self.args['AllocationSize']))

            if rest > 0:
                logging.debug('Emitting ProdMgr:ResourcesAvailable with '+\
                    str(rest)+' Jobs') 
                self.ms.publish("ProdMgrInterface:ResourcesAvailable", \
                    str(rest))    
        if payload == self.args['RandomCheck']['CheckQueue']:
            self.args['RandomCheck']['CheckQueue'] =str(random.random())
            logging.debug("Checking queue again in (HH:MM:SS): "+str(interval))
            self.ms.publish("ProdMgrInterface:CheckQueue", \
                str(self.args['RandomCheck']['CheckQueue']), interval)
        
    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        try:
            # create message service
            self.ms = MessageService()
            self.trigger=Trigger(self.ms)


            for handlerName in Registry.HandlerRegistry.keys():
               handler=retrieveHandler(handlerName)
               handler.ms = self.ms
               handler.trigger = self.trigger
               handler.args = self.args

    
            # register
            self.ms.registerAs("ProdMgrInterface")
    
            logging.debug("Subscribing to messages: ")
            # subscribe to messages
            self.ms.subscribeTo("ProdMgrInterface:StartDebug")
            self.ms.subscribeTo("ProdMgrInterface:EndDebug")
            self.ms.subscribeTo("ProdMgrInterface:AddRequest")
            self.ms.subscribeTo("ProdMgrInterface:AcquireRequests")
            self.ms.subscribeTo("ProdMgrInterface:CheckQueue")
            self.ms.subscribeTo("ProdMgrInterface:ResourcesAvailable")
            self.ms.subscribeTo("ProdMgrInterface:SetLocations")
            self.ms.subscribeTo("ProdMgrInterface:JobCutSize")
            self.ms.subscribeTo("JobSuccess")
            self.ms.subscribeTo("ProdMgrInterface:JobSuccess")
            self.ms.subscribeTo("GeneralJobFailure")
            self.ms.subscribeTo("ProdMgrInterface:SetJobCleanupFlag")
            self.ms.subscribeTo("ProdMgrInterface:CleanWorkflow")
            self.ms.subscribeTo("ProdMgrInterface:IncreaseEvents")
            logging.debug("Subscription completed ")
            
            # emit a acquire requests message.
            # add a random number to distinguish between requests injected
            # by persons and by this component.
            self.args['RandomCheck'] = {}
            self.args['RandomCheck']['AcquireRequests'] = str(random.random()) 
            self.args['RandomCheck']['CheckQueue'] = str(random.random()) 
            self.ms.remove("ProdMgrInterface:AcquireRequests")
            self.ms.remove("ProdMgrInterface:CheckQueue")
            self.ms.publish("ProdMgrInterface:AcquireRequests", \
                 str(self.args['RandomCheck']['AcquireRequests']))
            self.ms.publish("ProdMgrInterface:SetLocations",'')
            self.ms.commit()
            self.ms.publish("ProdMgrInterface:CheckQueue", \
                 str(self.args['RandomCheck']['CheckQueue']))
            self.ms.commit()
            logging.debug('Setting database access parameters')
            Session.set_database(dbConfig)
 
            # wait for messages
            while True:
                # SESSION: the message service uses the current session
                type, payload = self.ms.get()
                logging.debug("Receiving message of type: "+str(type)+\
                   ", payload: "+str(payload))
                Session.set_database(dbConfig)
                Session.connect()
                Session.start_transaction()
                self.__call__(type, payload)
                # we want to commit after the call has been sucessfuly completed
                # as this message will tell us the state of the component when
                # it crashed.

                # SESSION: when the message service commits it uses the current session
                # SESSION: this enables us to events and committing them without
                # SESSION: committing the event that initiated the handler.
                logging.debug("Closing all database sessions")
                # SESSION: commit and close remaining sessions
                Session.commit_all()
                Session.close_all()
                logging.debug("Committing message of type "+str(type))
                self.ms.commit()
                logging.debug("Finished handling message of type "+str(type))
        except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise
