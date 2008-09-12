#!/user/bin/env python
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

from ErrorHandler.Registry import retrieveHandler
from ErrorHandler.Registry import Registry 
from MessageService.MessageService import MessageService



class ErrorHandlerComponent:
    """
    _ErrorHandlerComponent_

    ProdAgent Component that responds to Error events. Depending
    on the type of handler that is associated to a particular event,
    a particular error handler is invoked.  For example the default 
    job failure error handler either submits a request for renewed 
    submission or cleanout the job information stored in the database 
    and contact the ProdManager (through an event submission).

    """

    def __init__(self, **args):
         """
         __init__
 
         initialization of the component. This methods defines
         to what events this components subscribes and initializes
         the logging for this component.

         """
         self.args = {}
         # if nothing is set, the location for storing the job
         # reports when there is a failure will be in the tmp dir.
         self.args['jobReportLocation']='/tmp/prodAgentJobReports'
         self.args['Logfile'] = None
         self.args.update(args)
 

         # the error events this components subscribes to
         # that invoke an error handler
         self.args['Events']={'JobFailed':'runFailureHandler', \
                              'SubmissionFailed':'submitFailureHandler', \
                              'CreateFailed':'createFailureHandler'}

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
                  handler=retrieveHandler(self.args['Events'][event])
                  handler.handleError(payload)
         except Exception, ex:
              logging.error("Failed to handle %s event with payload: %s" \
                            %(event,str(payload)))
              logging.error("Details: %s" % str(ex)) 
                
    def publishEvent(self,name,payload):
        """
        _publishEvent_
         
        Method called by the handlers if they need to publish an event.
        This method automatically chooses the message service consistent
        with its configuration.

        """   
        self.ms.publish(name,payload)
        self.ms.commit()

 
    def startComponent(self):
         """
         _startComponent_
 
         Start up the component
 
         """
         # prepare handlers:
         for handlerName in Registry.HandlerRegistry.keys():
             handler=Registry.HandlerRegistry[handlerName]
             handler.publishEvent=self.publishEvent
             if (handlerName == "runFailureHandler"):
                 handler.setJobReportLocation(self.args['jobReportLocation'])
                 
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
         self.ms.commit()
         # wait for messages
         while True:
             type, payload = self.ms.get()
             self.ms.commit()
             logging.debug("ErrorHandler: %s, %s" % (type, payload))
             self.__call__(type, payload)
