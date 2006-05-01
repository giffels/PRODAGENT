#!/usr/bin/env python

from Trigger.Registry import Registry
from Trigger.Registry import retrieveAction
from Trigger.Database.Api import TriggerAPIMySQL

class TriggerAPI:

   def __init__(self,messageService):
      self.messageService=messageService

      # prepare actions to work with this message service

      for actionName in Registry.ActionRegistry.keys():
           action=Registry.ActionRegistry[actionName]
           action.messageService=self.messageService
          

   def setFlag(self,triggerId,jobSpecId,flagId):
      """
      _setFlag_
   
      Sets a flag of a trigger associated to a jobspec. If this 
      is the last flag to be set (all other flags associated to this
      trigger have been set) the action will be invoked.
   
      input:
      -triggerId (string). Id of the trigger
      -flagId (string). Id of the flag
      -jobSpecId (string). Id of the job specification
   
      output:
      
      nothing or an exception if either the flag, trigger or jobSpec id
      does not exists.
   
      """
      if TriggerAPIMySQL.setFlag(triggerId,jobSpecId,flagId):
          try:
              actionName=TriggerAPIMySQL.getAction(triggerId,jobSpecId)
              action=retrieveAction(actionName)
              action.invoke()
          except Exception, ex:
              pass

    
   def resetFlag(self,triggerId,jobSpecId,flagId):
      """
      _resetFlag_
   
      Resets a flag. If all flags have been set the associated
      action will be triggered. Resetting a flag before all flags
      are set delays this if and only if the flag you reset is reset
      before all flags have been set.

      input:
      -triggerId (string). Id of the trigger
      -flagId (string). Id of the flag
      -jobSpecId (string). Id of the job specification
   
      output:
      
      nothing or an exception if either the flag, trigger or jobSpec id
      does not exists.
   
      """
   
      TriggerAPIMySQL.resetFlag(triggerId,jobSpecId,flagId)
   
   def flagSet(self,triggerId,jobSpecId,flagId):
      """
      _flagSet_

      -triggerId (string). Id of the trigger
      -flagId (string). Id of the flag
      -jobSpecId (string). Id of the job specification
   
      Returns true/false if the flag has been set or not, or an exception
      if the flag does not exists.
   
      """
   
      return TriggerAPIMySQL.flagSet(triggerId,jobSpecId,flagId)
   
   def allFlagSet(self,triggerId,jobSpecId):
      """
      _allFlagSet_
   
      -triggerId (string). Id of the trigger
      -jobSpecId (string). Id of the job specification
   
      Returns true/false to wheter all the flags have been
      set or not. 
   
      """

      return TriggerAPIMySQL.allFlagSet(triggerId,jobSpecId)
   
   
   def addFlag(self,triggerId,jobSpecId,flagId):
      """
      _addFlag_
      
      Adds a flag to a trigger. If this is the first flag for this
      trigger a new trigger will be created.

      input:
      -triggerId (string). Id of the trigger
      -flagId (string). Id of the flag
      -jobSpecId (string). Id of the job specification
   
      output:
      
      nothing or an exception if the flag already existed.
   
      """
   
      TriggerAPIMySQL.addFlag(triggerId,jobSpecId,flagId)
   

   def setAction(self,jobSpecId,triggerId,actionName):
      """
      _setAction_

      Sets the associated action that will be called
      if all flags are set. This action is registered in the action
      registery. If this trigger already had an action, this action
      will replace it.

      input:
      -triggerId (string). Id of the trigger
      -actionName (string). Name of the action

      output:
      nothing or an exception if the trigger does not exists.

      output:
   
      nothing or an exception if the flag already existed.

      """

      TriggerAPIMySQL.setAction(jobSpecId,triggerId,actionName)


   def cleanout(self,jobSpecId):
      """
      _cleanout_
  
      Removes all triggers associated to a jobSpecId

      """ 
      TriggerAPIMySQL.cleanout(jobSpecId)

      
