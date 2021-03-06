#!/user/bin/env python
"""
_JobCleanupComponent_

The JobCleanup subcribes to cleanup events. Currently there are two 
types of cleanups:

-JobCleanup : cleans upd the database entries and the cache dir
-PartialJobCleanup : selectively cleans up the cache dir to prevent
it from growing to large.
 
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import socket

from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.Trigger.Trigger import Trigger
from ProdCommon.Database import Session
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import GlobalRegistry



class JobCleanupComponent:
    """
    _JobCleanupComponent_

    ProdAgent Component that responds to cleanup events. Depending
    on the type of handler that is associated to a particular event,
    a particular cleanup handler is invoked.  For example the default 
    cleanup handler removes the job cache and cleans the database.

    """

    def __init__(self, **args):
         """
         __init__
 
         initialization of the component. This methods defines
         to what events this components subscribes and initializes
         the logging for this component.

         """
         self.args = {}
         self.args['Logfile'] = None
         self.args['FailureArchive'] = None
#         self.args['SuccessArchive'] = None
         self.args['KeepLogsInSuccessArchive'] = False
         self.args.update(args)
 

         # the cleanup events this components subscribes to
         # that invoke an cleanup handler
         self.args['Events']={'JobCleanup':'cleanupHandler',
                              'PartialJobCleanup':'partialCleanupHandler',\
                              'FailureCleanup':'failureCleanupHandler'}

         if self.args['Logfile'] == None:
              self.args['Logfile'] = os.path.join(self.args['ComponentDir'],\
                                                "ComponentLog")

         logHandler = RotatingFileHandler(self.args['Logfile'], "a", 1000000, 3)
         logFormatter = logging.Formatter("%(asctime)s:%(message)s")
         logHandler.setFormatter(logFormatter)
         logging.getLogger().addHandler(logHandler)
         logging.getLogger().setLevel(logging.INFO)


    def __call__(self, event, payload):
         """
         _operator()_

         Define response to an Event and payload

         """
         logging.debug("Received event: "+ str(event)+ \
                       " with payload: "+str(payload))

         try:
              if event == "JobCleanup:StartDebug":
                   logging.getLogger().setLevel(logging.DEBUG)
                   logging.info("logging level changed to DEBUG")
                   return
              elif event == "JobCleanup:EndDebug":
                   logging.getLogger().setLevel(logging.INFO)
                   logging.info("logging level changed to INFO")
                   return
              elif event in self.args['Events'].keys():
                  handler=retrieveHandler(self.args['Events'][event],"JobCleanup")
                  handler.handleEvent(payload)
         except Exception, ex:
              logging.error("Failed to handle %s event with payload: %s" \
                            %(event,str(payload)))
              logging.error("Details: %s" % str(ex)) 
                
    def publishEvent(self,name,payload):
        """
        _publishEvent_
         
        Method called by the handlers if they need to publish an event.
        This method automatically chooses the message service consistent
        with its configuration and commits the publication.

        """   
        self.ms.publish(name,payload)
        self.ms.commit()

 
    def startComponent(self):
         """
         _startComponent_
 
         Start up the component
 
         """
         # create message service
         self.ms = MessageService()
         self.trigger=Trigger(self.ms)
         # prepare handlers:
         for handlerName in GlobalRegistry.registries["JobCleanup"].keys():
             handler=GlobalRegistry.registries["JobCleanup"][handlerName]
             handler.publishEvent=self.publishEvent
             handler.failureArchive=self.args['FailureArchive']
             handler.successArchive=self.args['SuccessArchive']
             handler.keepLogsInSuccessArchive=self.args['KeepLogsInSuccessArchive']
             handler.trigger = self.trigger
                 
         # main body using persistent based message server
         logging.info("JobCleanup persistent based message service Starting...")
         msg="Success Archive: %s" % handler.successArchive
         logging.info(msg)
         msg="Failure Archive: %s" % handler.failureArchive
         logging.info(msg)
         if handler.keepLogsInSuccessArchive:
           logging.info("Set to keep logfiles in successful job archive")         

         
         # register
         self.ms.registerAs("JobCleanup")
         # subscribe to messages
         for event in self.args['Events'].keys():
             self.ms.subscribeTo(event)
         # Live controls for tweaking properties of this component
         # while alive
         self.ms.subscribeTo("JobCleanup:StartDebug")
         self.ms.subscribeTo("JobCleanup:EndDebug")
         self.ms.commit()
         # wait for messages
         while True:
             Session.set_database(dbConfig)
             Session.connect()
             Session.start_transaction()

             type, payload = self.ms.get()
             logging.debug("JobCleanup: %s, %s" % (type, payload))
             self.__call__(type, payload)
             # we only want to commit if the cleanup or archiving succeeds
             # and is not interupted by a crash of the prodagent.
             self.ms.commit()
             Session.commit_all()
             Session.close_all()

