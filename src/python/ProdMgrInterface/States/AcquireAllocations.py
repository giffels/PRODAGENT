#!/usr/bin/env python

import logging 

from ProdAgentCore.Codes import errors
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB import Session
from ProdMgrInterface import Allocation
from ProdMgrInterface import Cooloff
from ProdMgrInterface import MessageQueue
from ProdMgrInterface import Job
from ProdMgrInterface import Request
from ProdMgrInterface import State
from ProdMgrInterface.Registry import registerHandler
from ProdMgrInterface.States.StateInterface import StateInterface 
import ProdMgrInterface.Interface as ProdMgrAPI


class AcquireAllocations(StateInterface):

   def __init__(self):
       StateInterface.__init__(self)

   def execute(self):
       logging.debug("Executing state: AcquireAllocations")
       stateParameters=State.get("ProdMgrInterface")['parameters']
       numberOfJobs=stateParameters['numberOfJobs']

       # check how many allocations we have from this request:
       logging.debug("Checking allocations for request "+str(stateParameters['RequestID']))
       idleRequestAllocations=Allocation.get('prodagentLevel','idle',str(stateParameters['RequestID']))
       if ( ( (len(idleRequestAllocations)+Allocation.size("messageLevel")) )<numberOfJobs):
           logging.debug("Not enough idle allocations for request "+stateParameters['RequestID']+\
               ", will acquire more")
           
           logging.debug("Contacting: "+stateParameters['ProdMgrURL']+" with payload "+\
               stateParameters['RequestID']+','+str(int(numberOfJobs)-len(idleRequestAllocations)-Allocation.size("messageLevel")))
           # only request the extra allocations if we have spare idle ones.
           # the return format is an array of allocations ids we might get
           # less allocations back then we asked for, if there are not
           # enought available.

           # if we crashed we might not have retrieved the last call.
           if stateParameters['stateType']=='recover':
               try:
                   allocations=ProdMgrAPI.retrieve(stateParameters['ProdMgrURL'],"acquireAllocation","ProdMgrInterface")
               except ProdAgentException,ex:
                   if ex['ErrorNr']==3000:
                       logging.debug("No uncommited service calls: "+str(ex))
                       stateParametes['stateType']='normal'
           elif stateParameters['stateType']=='normal':
               try:
                   allocations=ProdMgrAPI.acquireAllocation(stateParameters['ProdMgrURL'],\
                       stateParameters['RequestID'],\
                       int(numberOfJobs)-len(idleRequestAllocations)-Allocation.size("messageLevel"),\
                       "ProdMgrInterface")
               except Exception,ex:
                   if ex.faultCode==2009:
                       Session.rollback()
                       logging.debug("This prodagent is set in cooloff state with "+stateParameters['ProdMgrURL'])
                       logging.debug("Trying to acquire other allocations")
                       Cooloff.insert(stateParameters['ProdMgrURL'],"00:10:00")
                       stateParameters['requestIndex']+=1
                       State.setParameters("ProdMgrInterface",stateParameters)
                       componentState="AcquireRequest"
                       State.setState("ProdMgrInterface",componentState)
                       Session.commit()
                       return componentState
                   # there is a problem connecting wrap up all relevant information and put
                   # it in a queue for later inspection
                   # NOTE: we need to distinguish between coolof exceptions and other errors.
                   # NOTE: we need to proper handle other exceptions (e.g. move things to a proper state)
                   #
                   # on this level there might be a connection problem. We do not need to save any state
                   # information yet but can just throw it in the cooloff as this is the first call in the 
                   # the handler. 
                   Session.rollback()
                   logging.debug("Problem connecting to server "+stateParameters['ProdMgrURL']+" : "+str(ex))
                   Cooloff.insert(stateParameters['ProdMgrURL'],"00:01:00")
                   stateParameters['requestIndex']+=1
                   State.setParameters("ProdMgrInterface",stateParameters)
                   componentState="AcquireRequest"
                   State.setState("ProdMgrInterface",componentState)
                   Session.commit()
                   return componentState


           stateParameters['stateType']='normal'
           # check if we got allocations back
           if type(allocations)==bool:
               if not allocations:
                  # everything is allocated for this request
                  stateParameters['requestIndex']+=1
                  State.setParameters("ProdMgrInterface",stateParameters) 
                  componentState="AcquireRequest"
                  State.setState("ProdMgrInterface",componentState)
                  Session.commit()
                  return componentState
           else:
               logging.debug("Acquired allocations: "+str(allocations))
               # we acquired allocations, update our own accounting:
               Allocation.insert("prodagentLevel",allocations,stateParameters['RequestID'])
               idleRequestAllocations=Allocation.get('prodagentLevel','idle',(stateParameters['RequestID']))
               Allocation.insert("messageLevel",idleRequestAllocations,stateParameters['RequestID'])
               Allocation.insert("requestLevel",idleRequestAllocations,stateParameters['RequestID'])
           # we can commit as we made everything persistent at the client side.
           ProdMgrAPI.commit()
       else:
           diff=numberOfJobs-len(Allocation.size("WorkfAllocation"))
           Allocation.insert("messageLevel",idleRequestAllocations[0:diff])
           Allocation.insert("requestLevel",idleRequestAllocations[0:diff])
           logging.debug("Sufficient allocations acquired, proceeding with acquiring jobs")
       componentState="AcquireJobs"
       State.setParameters("ProdMgrInterface",stateParameters)
       State.setState("ProdMgrInterface",componentState)
       Session.commit()
       return componentState


registerHandler(AcquireAllocations(),"AcquireAllocations")







