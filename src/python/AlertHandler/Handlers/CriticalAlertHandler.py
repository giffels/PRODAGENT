#!/usr/bin/env python


import logging
from ProdCommon.Core.GlobalRegistry import registerHandler
from AlertHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.Alert.AlertPayload import AlertPayload
from ProdCommon.Database.Operation import Operation
from ProdAgentDB.Config import defaultConfig as dbConfig
  

class CriticalAlertHandler (HandlerInterface):
      """
      _CriticalAlertHandler_
      CriticalAlertHandler performs action in response to CriticalAlert event. The payload information should be passed to 
      HTTPFrontend so that Operations can take instant action upon receiving alert 
      """ 
      def __init__ (self):
          """
          _init_
          
          Constructor  
          """
          HandlerInterface.__init__(self)
          self.alertDBOperations = None

          logging.debug ('CriticalAlertHandler Initialized...')

         

      def handleError (self, payload):
          """
          _handleError_         

          """
          logging.debug('\n\nCriticalAlertHandler is handling Payload: '+payload)

          #// Handle Payload
          self.alertDBOperations = Operation(dbConfig)

          alertPayload = AlertPayload()
          alertPayload.load(payload)
          alertPayload['Severity'] = 'critical'

          tableName = 'alert_current'
          sqlStr = "insert into " + tableName + " (type, component, message, time) values ( \'" + str(alertPayload['Severity'])+ "\',\'" + str(alertPayload['Component']) + '\',\'' + str(alertPayload['Message']) + '\',\'' + str(alertPayload['Time']) + "\')" 

          self.alertDBOperations.execute (sqlStr)
          self.alertDBOperations.commit()         
           
          logging.debug(alertPayload) 

       
          return 

        	  

registerHandler (CriticalAlertHandler(), 'CriticalAlertHandler', 'AlertHandler')     

