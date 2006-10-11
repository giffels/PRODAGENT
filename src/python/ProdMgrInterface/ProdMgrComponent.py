#!/usr/bin/env python
"""
_ProdMgrComponent_

Component that communicates with the ProdMgr to retrieve work
and report back details of completed jobs



"""


import os
import time

import logging
from logging.handlers import RotatingFileHandler

from MessageService.MessageService import MessageService

from ProdAgentDB import Session
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.Codes import errors

from ProdMgrInterface.Registry import retrieveHandler
from ProdMgrInterface.Registry import Registry

from ProdMgrInterface import Allocation
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State

import ProdMgrInterface.Interface as ProdMgrAPI


class ProdMgrComponent:
    """
    _ProdMgrComponent_

    Component that interacts with the ProdMgr

    """
    def __init__(self, **args):

       try:
            self.args = {}
            self.args['Logfile'] = None
            self.args.update(args)
            if self.args['Logfile'] == None:
               self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                    "ComponentLog")
            #  //
            # // Log Handler is a rotating file that rolls over when the
            #//  file hits 1MB size, 3 most recent files are kept
            logHandler = RotatingFileHandler(self.args['Logfile'],
                                             "a", 1000000, 3)
            #  //
            # // Set up formatting for the logger and set the
            #//  logging level to info level
            logFormatter = logging.Formatter("%(asctime)s:%(module)s:%(message)s")
            logHandler.setFormatter(logFormatter)
            logging.getLogger().addHandler(logHandler)
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info("ProdMgrComponent Started...")
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
        if event == "ResourcesAvailable":
            try:
                payloadVal = int(payload)
            except ValueError:
                payloadVal = 1
            self.retrieveWork(payloadVal)
            return

        if event == "JobSuccess":
            self.reportJobSuccess(payload)
            return

        if event == "GeneralJobFailure":
            self.reportJobFailure(payload)
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
           Session.set_current("ProdMgrInterface")
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
               componentState="EvaluateAllocations"
           Session.commit()
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
            logging.debug("Add request: "+requestId+" with priority "+priority+" for prodmgr: "+prodMgr)
            Request.insert(requestId,priority,prodMgr)
            logging.debug("Added request. There are now "+str(Request.size())+" requests in the queue ")
        except Exception,ex:
            logging.debug("ERROR "+str(ex))



    def reportJobSuccess(self, frameworkJobReport):
        """
        _reportJobSuccess_

        Read the report provided and report the details back to the
        ProdMgr
        
        """
        logging.debug("Reporting Job Success "+frameworkJobReport)
        Session.connect("ProdMgrInterface")
        Session.set_current("ProdMgrInterface")
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
                componentStateInfo['parameters']['stateType']='normal'
                componentStateInfo['parameters']['jobType']='success'
                componentStateInfo['parameters']['jobReport']=frameworkJobReport
                State.setParameters("ProdMgrInterface",componentStateInfo['parameters'])
        logging.debug("ProdMgrInterface state is: "+str(componentStateInfo['state']))

        # first check if there are any queued events as a prodmgr might have been offline
        componentState=componentStateInfo['state']
        if componentStateInfo['state']=='start':
            componentState="QueuedJobResults"
        Session.commit()
        while componentState!='start':
            state=retrieveHandler(componentState)
            componentState=state.execute()
        logging.debug("reportJobSuccess event handled")

    def reportJobFailure(self, frameworkJobReport):
        """
        _reportJobFailure_

        Read the report provided and report the details back to the ProdMgr

        """
        logging.debug("Reporting Job Failure"+frameworkJobReport)
        Session.connect("ProdMgrInterface")
        Session.set_current("ProdMgrInterface")
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
                componentStateInfo['parameters']['stateType']='normal'
                componentStateInfo['parameters']['jobType']='failure'
                componentStateInfo['parameters']['jobReport']=frameworkJobReport
                State.setParameters("ProdMgrInterface",componentStateInfo['parameters'])
        logging.debug("ProdMgrInterface state is: "+str(componentStateInfo['state']))

        # first check if there are any queued events as a prodmgr might have been offline
        componentState=componentStateInfo['state']
        if componentStateInfo['state']=='start':
            componentState="QueuedJobResults"
        Session.commit()
        while componentState!='start':
            state=retrieveHandler(componentState)
            componentState=state.execute()
        logging.debug("reportJobFailure event handled")
        
        
        
        

    def startComponent(self):
        """
        _startComponent_

        Start up the component

        """
        try:
            # create message service
            self.ms = MessageService()

            for handlerName in Registry.HandlerRegistry.keys():
               handler=retrieveHandler(handlerName)
               handler.ms=self.ms

    
            # register
            self.ms.registerAs("ProdMgrInterface")
    
            logging.debug("Subscribing to messages: ")
            # subscribe to messages
            self.ms.subscribeTo("ProdMgrInterface:StartDebug")
            self.ms.subscribeTo("ProdMgrInterface:EndDebug")
            self.ms.subscribeTo("ProdMgrInterface:AddRequest")
            self.ms.subscribeTo("ResourcesAvailable")
            self.ms.subscribeTo("JobSuccess")
            self.ms.subscribeTo("GeneralJobFailure")
            logging.debug("Subscription completed ")
            
            
            # wait for messages
            while True:
                # SESSION: the message service uses the current session
                Session.connect()
                Session.start_transaction()

                type, payload = self.ms.get()
                logging.debug("Message type: "+str(type)+", payload: "+str(payload))
                self.__call__(type, payload)
                # we want to commit after the call has been sucessfuly completed
                # as this message will tell us the state of the component when
                # it crashed.

                # SESSION: when the message service commits it uses the current session
                # SESSION: this enables us to events and committing them without
                # SESSION: committing the event that initiated the handler.
                logging.debug("Committing Event "+str(type))
                self.ms.commit()

                # SESSION: commit and close remaining sessions
                Session.commit_all()
                Session.close_all()
                logging.debug("Finished handling event of type "+str(type))
        except Exception,ex:
            logging.debug("ERROR: "+str(ex))     
            raise
