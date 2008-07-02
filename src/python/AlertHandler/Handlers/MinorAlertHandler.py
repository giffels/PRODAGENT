#!/usr/bin/env python

import logging
from ProdCommon.Core.GlobalRegistry import registerHandler
from AlertHandler.Handlers.HandlerInterface import HandlerInterface
from ProdCommon.Database.Operation import Operation
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Alert.AlertPayload import AlertPayload
    
    
class MinorAlertHandler (HandlerInterface):
      """
      _MinorAlertHandler_
      MinorAlertHandler performs action in response to MinorAlert event. The payload information should be passed to 
      log file
      """ 
      def __init__ (self):
          """
          _init_
          
          Constructor  
          """
          HandlerInterface.__init__(self)
          self.alertDBOperations = None

          logging.debug ('MinorAlertHandler Initialized...')


      def handleError (self, payload):
          """
          _handleError_         

          """
          logging.debug('\n\nMinorAlertHandler is handling Payload: '+payload)

          #// Handle Payload
          self.alertDBOperations = Operation(dbConfig)

          alertPayload = AlertPayload()
          alertPayload.load(payload)
          alertPayload['Severity'] = 'minor'


          tableName = 'alert_current'
          sqlStr = "insert into " + tableName + " (type, component, message, time) values ( \'" + str(alertPayload['Severity'])+ "\',\'" + str(alertPayload['Component']) + '\',\'' + str(alertPayload['Message']) + '\',\'' + str(alertPayload['Time']) + "\')"

          self.alertDBOperations.execute (sqlStr)
          self.alertDBOperations.commit()         
           
          logging.debug(alertPayload) 

       
          return 


registerHandler (MinorAlertHandler(), 'MinorAlertHandler', 'AlertHandler')     

