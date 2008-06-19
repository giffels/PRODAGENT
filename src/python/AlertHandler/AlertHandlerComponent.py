#!/usr/bin/env python

"""
_AlertHandlerComponent_

AlertHandlerComponent subscribes to Alert Events. The payload consists of a file name in which object of AlertPayload Class is pickled. AlertPayload has attributes which Severity of Alert (i.e Critical, Error, Warning, Minor), Message consists of Alert message/cause of Exception, Component consists of name of component from which Alert was being published. 
AlertPayload is located in ProdCommon.Alert.AlertPayload. Alert can be published as:
alert = AlertPayload(**{'Severity':'warning,'Message':'Test','Component':'Any Component',})
alert.save()
ms.publish('WarningAlert',alert.FileName)

Publishing Alert types can be one of the following:

CriticalAlert
ErrorAlert
WarningAlert
MinorAlert

Severity of AlertPayload can be one of the following: [Optional]

critical
error
warning
minor
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from ProdCommon.Core.GlobalRegistry import GlobalRegistry
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbconfig
from ProdCommon.Database import Session
from ProdCommon.Alert.AlertPayload import AlertPayload


class AlertHandlerComponent:
   """
   _AlertHandlerComponent_

   AlertHandler class that subscribes to alert events and then binds the alert events to respective alert handlers. Upon 
   reciept of certain alert event it invokes the alert handler which handles that alert event    
   """

   def __init__ (self, **args):

       """
       __init__
       Initilze this component. This method will define the alert events with whom this component will subscribes to.
       This method also initialzes the logging module.

        """

       #// Define default parameters  
       self.args = {}
       self.args['Logfile'] = None

       #// Update parameters list with User defined parameters in ProdAgent Configuration 
       self.args.update (args)       
       
       #// Initialize logging. Logging message includes time, file and function name in which logging message was thrown             
       if self.args['Logfile'] == None:
          self.args['Logfile'] = os.path.join(self.args['ComponentDir'], 'ComponentLog')
    
    
       logHandler = RotatingFileHandler (self.args['Logfile'], 'a', 1000000, 15)
       format = logging.Formatter ('%(asctime)s:%(message)s')
       logHandler.setFormatter (format)
       logging.getLogger().addHandler (logHandler)
       logging.getLogger().setLevel (logging.INFO)

       #// Define Alert Events with with this component subscribes to  
       self.args['AlertEvent'] = {
                                    'MinorAlert':'MinorAlertHandler',
                                    'WarningAlert':'WarningAlertHandler',           
                                    'ErrorAlert':'ErrorAlertHandler',
                                    'CriticalAlert':'CriticalAlertHandler'

                                   }
    
       #// Pick up the handler if User provided in prodagent configuration    
       for handler in self.args['AlertEvent'].keys():

           if self.args.has_key(handler):

              self.args['AlertEvent'][handler] = self.args[handler]    

       return         

   def __call__ (self, event, payload):
       """
       Method that responds to the caught event

       """

       logging.info ('\n\nReceived Event: '+ str(event) + '\nPayload: ' + str(payload))

       try:

          if event == 'AlertHandler:StartDebug':
             logging.getLogger().setLevel(logging.DEBUG)
             logging.info ('Logging level changed to DEBUG Mode')

          elif event == 'AlertHandler:EndDebug':
               logging.getLogger().setLevel(logging.INFO)
               logging.info ('Logging level changed to INFO Mode')
 
          elif event in self.args['AlertEvent'].keys():
               handler = retrieveHandler(self.args['AlertEvent'][event],'AlertHandler')
               handler(payload)

       except Exception, ex:  
  
          logging.error('Exception Caught while handling the event: ' + str(event) + ' payload: ' + str(payload) )      
          logging.error(str(ex))

       return 


   def startComponent (self):
       """
       _startComponent_

       AlertHandler component starter function
       """
 
       logging.info ('\n\nStarting AlertHandler Component...')

       #// Message service object for this component
       self.ms = MessageService()
       
       #// Register this component to MessageService
       self.ms.registerAs('AlertHandler')
 
       logging.debug ('Assiging important object references to all the alert handlers registered in PRODCOMMON')
       
       for handler in GlobalRegistry.registries['AlertHandler'].keys():
 
           handlerRef = GlobalRegistry.registries['AlertHandler'][handler]
           handlerRef.ms = self.ms
           handlerRef.args = self.args
           
       
       #//Subscribing to all defined Alert Events 
       for event in self.args['AlertEvent'].keys():

           self.ms.subscribeTo (event) 
           logging.info('Subscribed to %s event' % event)
       
       #// Subscribing to Debug log control event 
       self.ms.subscribeTo('AlertHandler:StartDebug')
       self.ms.subscribeTo('AlertHandler:EndDebug')
       self.ms.commit()  
       self.ms.publish('AlertHandler:StartDebug','') 
       self.ms.commit()

       logging.debug('Subscribed to StartDebug & EndDebug events')
       logging.info ('AlertHandler Component started Successfully')

 

       #// Start Listning to message service events
       while True: 
       
             Session.set_database(dbconfig)
             Session.connect()
             Session.start_transaction()
             type, payload = self.ms.get()


             #//passing Event payload to event responsive function 
             self.__call__ (type, payload)           

             #//Committing transaction and start listning to published events again 
             Session.commit_all()
             Session.close_all()
             self.ms.commit() 
 
       return




       
