#!/usr/bin/env python
"""
_ErrorHandlerComponent_

Skeleton ErrorHandlerComponent

The ErrorHandler subcribes to Error events. The payload of an error event
varies. For a job failure event the payload consists of a job report
from which we extract the job spec id. Other error events will have
different payloads. Depending on the type of error event the appropiate
error handler will be loaded for handling the event. 

"""

import logging
from logging.handlers import RotatingFileHandler
import os
import socket

from MessageService.MessageService import MessageService

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import GlobalRegistry


class ErrorHandlerComponent:
    """
    _ErrorHandlerComponent_

    ProdAgent Component that responds to Error events. Depending
    on the type of handler that is associated to a particular event,
    a particular error handler is invoked.  For example the default 
    job failure error handler either submits a request for renewed 
    submission or cleanout the job information stored in the database 
    and contact the ProdManager (through an event submission). 

    If resubmission takes place, the size of the job cache is examined.
    if found to larget, it is selectively purged by submitting a 
    "PartialJobCleanup" event to the JobCleanup component.

    """

    def __init__(self, **args):
         """
         __init__
 
         initialization of the component. This methods defines
         to what events this component subscribes and initializes
         the logging for this component.

         """
         self.args = {}
         # if nothing is set, the location for storing the job
         # reports when there is a failure will be in the tmp dir.
         self.args['jobReportLocation'] = '/tmp/prodAgentJobReports'
         self.args['ReportAction'] = 'noMove'
         self.args['Logfile'] = None
         self.args['MaxCacheDirSizeMB'] = 100
         self.args['DelayFactor'] = 60
         self.args['QueueFailures'] = "False"
         # set default handler names which can be overwritten 
         # by config defined handlers.
         self.args['RunHandlerName'] = 'runFailureHandler'
         self.args['SubmitHandlerName'] = 'submitFailureHandler'
         self.args['CreateHandlerName'] = 'createFailureHandler'
         self.args.setdefault("HeartBeatDelay", "00:05:00")

         self.args.update(args)

         if len(self.args["HeartBeatDelay"]) != 8:
             self.HeartBeatDelay="00:05:00"
         else:
             self.HeartBeatDelay=self.args["HeartBeatDelay"]

         if self.args['QueueFailures'].lower() in ('true', 'yes'):
             self.args['QueueFailures'] = True
         else:
             self.args['QueueFailures'] = False

         # the error events this components subscribes to
         # that invoke an error handler
         self.args['Events']={'JobFailed':self.args['RunHandlerName'], \
                              'SubmissionFailed': self.args['SubmitHandlerName'], \
                              'CreateFailed': self.args['CreateHandlerName']}

         # check if we need to use non default handlers
         for handler in self.args['Events'].keys():

             # assign the non default handler if provided
             if self.args.has_key(handler):
                 self.args['Events'][handler] = self.args[handler]

         if self.args['Logfile'] == None:
              self.args['Logfile'] = os.path.join(self.args['ComponentDir'],\
                                                "ComponentLog")

         logHandler = RotatingFileHandler(self.args['Logfile'], "a", 1000000, 3)
         logFormatter = logging.Formatter("%(asctime)s:%(message)s")
         logHandler.setFormatter(logFormatter)
         logging.getLogger().addHandler(logHandler)
         logging.getLogger().setLevel(logging.INFO)


         # create the jobReportLocation if not exists.
         pipe=os.popen("mkdir -p "+self.args['jobReportLocation'])
         pipe.close()


    def __call__(self, event, payload):
         """
         _operator()_

         Define response to an Event and payload

         """
         logging.debug("Received event: "+ str(event)+ \
                       " with payload: "+str(payload))

         try:
              if event == "ErrorHandler:StartDebug":
                  logging.getLogger().setLevel(logging.DEBUG)
                  logging.info("logging level changed to DEBUG")
                  return
              elif event == "ErrorHandler:EndDebug":
                  logging.getLogger().setLevel(logging.INFO)
                  logging.info("logging level changed to INFO")
                  return
              elif event in self.args['Events'].keys():
                  handler=retrieveHandler(self.args['Events'][event],"ErrorHandler")
                  handler.handleError(payload)
              elif event == "ErrorHandler:HeartBeat":
                  logging.info("HeartBeat: I'm alive ")
                  self.ms.publish("ErrorHandler:HeartBeat","",self.HeartBeatDelay)
                  self.ms.commit()
              else:
                  logging.error("No handler available for %s event with payload: %s" \
                      %(event,str(payload)))
         except Exception, ex:
              logging.error("Failed to handle %s event with payload: %s" \
                            %(event,str(payload)))
              logging.error("Details: %s" % str(ex)) 
                
    def publishEvent(self,name,payload,delay="00:00:00"):
        """
        _publishEvent_
         
        Method called by the handlers if they need to publish an event.
        This method automatically chooses the message service consistent
        with its configuration and commits the publication.

        """   
        self.ms.publish(name,payload,delay)
        self.ms.commit()

 
    def startComponent(self):
         """
         _startComponent_
 
         Start up the component
 
         """
         # prepare handlers:
         for handlerName in GlobalRegistry.registries["ErrorHandler"].keys():
             handler=GlobalRegistry.registries["ErrorHandler"][handlerName]
             handler.publishEvent=self.publishEvent
             handler.args=self.args
             #NOTE: remove this
             handler.maxCacheDirSizeMB=self.args['MaxCacheDirSizeMB']
                 
         # main body using persistent based message server
         logging.info("ErrorHandler persistent based message service Starting...")
         
         # create message service
         self.ms = MessageService()
         
         # register
         self.ms.registerAs("ErrorHandler")
         # subscribe to messages
         for event in self.args['Events'].keys():
             self.ms.subscribeTo(event)
         # Live controls for tweaking properties of this component
         # while alive
         self.ms.subscribeTo("ErrorHandler:StartDebug")
         self.ms.subscribeTo("ErrorHandler:EndDebug")
         self.ms.subscribeTo("ErrorHandler:HeartBeat")
         self.remove("ErrorHandler:HeartBeat")
         self.ms.publish("ErrorHandler:HeartBeat","",self.HeartBeatDelay)
         self.ms.commit()
         # wait for messages
         while True:
             Session.set_database(dbConfig)
             Session.connect()
             Session.start_transaction()
             type, payload = self.ms.get()

             logging.debug("ErrorHandler: %s, %s" % (type, payload))
             self.__call__(type, payload)

             Session.commit_all()
             Session.close_all()
             self.ms.commit()
