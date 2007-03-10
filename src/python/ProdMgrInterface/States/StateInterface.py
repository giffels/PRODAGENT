#!/usr/bin/env python
import logging

from ProdCommon.Database import Session
from ProdMgrInterface import MessageQueue

class StateInterface:
    """
    _StateInterface_

    Common State Interface, State implementations should inherit 
    from this class and implement the execute method.
   
    """

    def __init__(self):
         """

         Constructor

         """

    def execute(self,stateParameters={}):
         """
         _execute_
         
         Handles the error based on the payload it receives.
         """

         msg = "Virtual Method StateInterface.execute called"
         raise RuntimeError, msg

    def storeMessage(self,message):
         MessageQueue.insert("ProdMgrInterface",message['state'],message['server_url'],\
             message['type'],\
             message['parameters'],"00:00:10")
         logging.debug("Problem connecting to server "+message['server_url'])
         logging.debug("Attempt stored in message queue for later retries")
         Session.commit()

    def __call__(self):
         """
         Call method
         """
