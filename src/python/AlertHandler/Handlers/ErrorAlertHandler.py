#!/usr/bin/env python

import logging
from ProdCommon.Core.GlobalRegistry import registerHandler
from AlertHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.Database.Operation import Operation
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Alert.AlertPayload import AlertPayload
    

class ErrorAlertHandler (HandlerInterface):
      """
      _ErrorAlertHandler_
      ErrorAlertHandler performs action in response to ErrorAlert event. The payload information should be passed to 
      logfile 
      """ 
      def __init__ (self):
          """
          _init_
          
          Constructor  
          """
          HandlerInterface.__init__(self)
	  self.alertDBOperations = Operation(dbConfig)
          logging.debug ('ErrorAlertHandler Initialized...')
	  
	
      def handleError (self, payload):
          """
          _handleError_         

          """
          logging.debug('\n\nErrorAlertHandler is handling Payload: '+payload)

          #// Handle Payload
          alertPayload = AlertPayload()
          alertPayload.load(payload)
          alertPayload['Severity'] = 'error'

          tableName = 'alert_current'
          sqlStr = "insert into " + tableName + " (type, component, message, time) values ( \'" + str(alertPayload['Severity'])+ "\',\'" + str(alertPayload['Component']) + '\',\'' + str(alertPayload['Message']) + '\',\'' + str(alertPayload['Time']) + "\')"

          self.alertDBOperations.execute (sqlStr)
          self.alertDBOperations.commit()         
           
          logging.debug(alertPayload) 

       
          return 


registerHandler (ErrorAlertHandler(), 'ErrorAlertHandler', 'AlertHandler')     

