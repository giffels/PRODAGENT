#!/usr/bin/env python

"""
Class that implements trigger functionality for
Different components to synchronize work
"""

import logging

from ProdAgent.Core.Codes import exceptions
from ProdCommon.Core.GlobalRegistry import GlobalRegistry
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Database import Session
from ProdCommon.Core.ProdException import ProdException

class Trigger:

    """
    Class that implements trigger functionality for
    Different components to synchronize work
    """

    def __init__(self, messageService):
        self.messageService = messageService
        # prepare actions to work with this message service
        for actionName in \
        GlobalRegistry.registries['ProdAgent.Triggers'].keys():
            action = GlobalRegistry.registries['ProdAgent.Triggers'][actionName]
            action.messageService = self.messageService

    def setFlag(self, triggerId, jobSpecId, flagId):
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
        msg = """
Setting triggerID/jobID/flagID %s / %s / %s 
        """ % (triggerId, jobSpecId, flagId)
        logging.debug(msg)
        logging.debug("Locking trigger rows")
        sqlStr = """ 
        SELECT * FROM tr_Trigger WHERE TriggerID='%s' AND
        JobSpecID='%s' FOR UPDATE
        """ % (triggerId, jobSpecId)
        Session.execute(sqlStr)
        #update flags
        sqlStr = """UPDATE tr_Trigger SET FlagValue="finished" WHERE
        TriggerID="%s" AND JobSpecID="%s" 
        AND FlagID="%s" ; """ % (triggerId, jobSpecId, flagId)
        rowsModified = Session.execute(sqlStr)
        if rowsModified == 0:
            raise ProdException(exceptions[3003]+':'+str(triggerId)+\
            ','+str(jobSpecId)+','+str(flagId),3003)
        #check if all flags are set:
        sqlStr = """SELECT COUNT(*) FROM (SELECT COUNT(*) as 
        total_count FROM tr_Trigger WHERE TriggerID="%s" AND JobSpecID="%s") 
        as total_count, (SELECT COUNT(*) as total_count FROM tr_Trigger 
        WHERE FlagValue="finished" AND TriggerID="%s" AND JobSpecID="%s") 
        as finished_count WHERE 
        total_count.total_count=finished_count.total_count; 
        """ % (triggerId, jobSpecId, triggerId, jobSpecId)
        Session.execute(sqlStr)
        rows = Session.fetchall()
        # if flags are set invoke action
        logging.debug("test "+jobSpecId+" "+str(rows))
        if rows[0][0] == 1:
            sqlStr = """SELECT ActionName FROM tr_Action WHERE TriggerID="%s" 
            AND JobSpecID="%s" ; """ % (triggerId, jobSpecId)
            Session.execute(sqlStr)
            rows = Session.fetchall()
            if len(rows) == 1:
                action = retrieveHandler(rows[0][0],'ProdAgent.Triggers')
            action.invoke(jobSpecId)

   
    def resetFlag(self, triggerId, jobSpecId, flagId):
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
        sqlStr = """UPDATE tr_Trigger SET FlagValue="start" WHERE
        TriggerID="%s" AND JobSpecID="%s" 
        AND FlagID="%s" """ % (triggerId, jobSpecId, flagId)
        rowsModified = Session.execute(sqlStr)
        if rowsModified == 0:
            raise ProdException(exceptions[3003]+":"+str(triggerId)+\
                ","+str(jobSpecId)+","+str(flagId),3003)
  
    def flagSet(self, triggerId, jobSpecId, flagId):
        """
        _flagSet_
  
        -triggerId (string). Id of the trigger
        -flagId (string). Id of the flag
        -jobSpecId (string). Id of the job specification
     
        Returns true/false if the flag has been set or not, or an exception
        if the flag does not exists.
     
        """
        sqlStr = """SELECT FlagValue FROM tr_Trigger WHERE TriggerID="%s" 
        AND FlagID="%s" AND JobSpecID="%s"; """ % (triggerId, flagId, jobSpecId)
        Session.execute(sqlStr)
        rows = Session.fetchall()
        if (len(rows) == 0):
            return False
        if rows[0][0] != "finished":
            return False
        return True
  
    def allFlagSet(self, triggerId, jobSpecId):
        """
        _allFlagSet_
     
        -triggerId (string). Id of the trigger
        -jobSpecId (string). Id of the job specification
     
        Returns true/false to wheter all the flags have been
        set or not. 
     
        """
        sqlStr =""" SELECT COUNT(*) FROM (SELECT COUNT(*) as total_count 
        FROM tr_Trigger WHERE TriggerID="%s" AND JobSpecID="%s") as 
        total_count, (SELECT COUNT(*) as total_count FROM tr_Trigger 
        WHERE FlagValue="finished" AND TriggerID="%s" AND JobSpecID="%s") 
        as finished_count WHERE 
        total_count.total_count=finished_count.total_count; 
        """ % (triggerId, jobSpecId, triggerId, jobSpecId)
        Session.execute(sqlStr)
        rows = Session.fetchall()
        if rows[0][0] == 1:
            return True
        return False
     
    def addFlag(self, triggerId, jobSpecId, flagId):
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
        try:
            sqlStr = """INSERT INTO 
            tr_Trigger(JobSpecID,TriggerID,FlagID,FlagValue)
            VALUES("%s","%s","%s","start") ;""" % (jobSpecId, triggerId, flagId)
            Session.execute(sqlStr)
        except:
            raise ProdException(exceptions[3003], 3003)
  
    def setAction(self, jobSpecId, triggerId, actionName):
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
        try:
            sqlStr = """INSERT INTO tr_Action(jobSpecId,triggerId,actionName)
            VALUES("%s","%s","%s") ;""" % (jobSpecId, triggerId, actionName)
            Session.execute(sqlStr)
        except Exception,ex:
            msg = ":"+str(jobSpecId)+","+str(triggerId)+","+str(actionName)
            raise ProdException(exceptions[3002]+msg+str(ex), 3002)

    def cleanout(self, jobSpecId):
        """
        _cleanout_
    
        Removes all triggers associated to a jobSpecId
  
        """ 
        sqlStr = """DELETE FROM tr_Trigger WHERE JobSpecID="%s" 
        """ % (jobSpecId)
        Session.execute(sqlStr)
        sqlStr = """DELETE FROM tr_Action WHERE JobSpecID="%s" 
        """ % (jobSpecId)
        Session.execute(sqlStr)
 
