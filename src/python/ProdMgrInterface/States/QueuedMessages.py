#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
from ProdMgrInterface import MessageQueue
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.Registry import retrieveHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI


class QueuedMessages(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: QueuedMessages")
       ignoreUrl={}

       for report_type in ['ReportJobSuccess']:
           start=0
           amount=10
           messages=MessageQueue.retrieve('ProdMgrInterface',report_type,start,amount)
           while(len(messages)>0):
               handler=retrieveHandler(report_type)
               for message in messages:
                   if not ignoreUrl.has_key(message['server_url']):
                       MessageQueue.remove(message['id'])
                       result=handler.sendMessage(message['server_url'],message['parameters'])
                       if result['url']=='failed':
                           ignoreUrl[message['server_url']]='failed'
                       message['parameters']['result']=result['result']
                       handler.handleResult(message['parameters'])
                   logging.debug("Retrieve next message in queue")
               start=start+amount
               messages=MessageQueue.retrieve('ProdMgrInterface',report_type,start,amount)
       logging.debug("Examined all messages in queue. Moving to next state")

registerHandler(QueuedMessages(),"QueuedMessages")







