#/usr/bin/env python


from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session

import logging


def amount():
   """
   __amount__

   returns the amount of workflows the PA currently works on.
   """
   sqlStr="""SELECT count(*) FROM we_Workflow"""
   Session.execute(sqlStr)
   rows=Session.fetchall()
   return rows[0][0]

def exists(workflowID):
   """
   __exists__

   returns true if an entry exists for this ID
   """
   sqlStr="""SELECT count(*) FROM we_Workflow WHERE 
   id="%s" """ %(str(workflowID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if rows[0][0]==1:
      return True
   return False

def get(workflowID=[]):
   """
   __getWorkflows__

   returns workflow entries
   """
   if(type(workflowID)!=list):
       workflowID=[str(workflowID)]
   if len(workflowID)==0:
       return
   if len(workflowID)==1:
       sqlStr="""SELECT events_processed,id,priority,prod_mgr_url,
       workflow_spec_file,workflow_type FROM we_Workflow WHERE id="%s"
       """ %(str(workflowID[0]))
   else:
       sqlStr="""SELECT events_processed,id,priority,prod_mgr_url,
       workflow_spec_file,workflow_type FROM we_Workflow WHERE id IN 
       %s """ %(str(tuple(workflowID)))
   Session.execute(sqlStr)
   description=['events_processed','id','priority','prod_mgr_url',\
   'workflow_spec_file','workflow_type']
   result=Session.convert(description,Session.fetchall())
   if len(result)==1:
      return result[0]
   return result

def getHighestPriority(nth=0):
   """
   ___getHighestPriority___

   gets the nth highest priority if exists.
   """
   sqlStr="""SELECT events_processed,id,priority,prod_mgr_url,
   workflow_spec_file,workflow_type FROM we_Workflow ORDER by priority limit %s
   """ %(str(nth+1))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if nth>(len(rows)-1):
        return []
   row=rows[nth]
   description=['events_processed','id','priority','prod_mgr_url',\
   'workflow_spec_file','workflow_type']
   return Session.convert(description,[row],True)

def getJobIDs(workflowIDs=[]):
   """
   __getJobsIDs__

   returns jobids associated to the list of workflowIDs
   """
   pass

def getNotDownloaded():
   """
   __getNotDownloaded__

   returns the workflows this PA should work on but from which the 
   workflow file has not been downloaded yet.
   """
   sqlStr="""SELECT events_processed,id,priority,prod_mgr_url,
   workflow_spec_file,workflow_type FROM we_Workflow 
   WHERE workflow_spec_file="not_downloaded" """
   Session.execute(sqlStr)
   description=['events_processed','id','priority','prod_mgr_url',\
   'workflow_spec_file','workflow_type']
   return Session.convert(description,Session.fetchall())
   
def isAllocationsFinished(workflowID):
   """
   __isAllocationsFinished__

   returns true if the allocations this PA was working on for this workflow are finshed.

   """
   sqlStr="""SELECT COUNT(*) FROM we_Allocation WHERE workflow_id='%s'
   """ %(str(workflowID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if rows[0][0]==0:
       return True
   return False

def isFinished(workflowID):
   """
   __isDone__

   returns true if this workflow id finished 
   """
   sqlStr="""SELECT count(*) FROM we_Allocation WHERE 
   workflow_id="%s" """ %(str(workflowID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if rows[0][0]==1:
      return False
   return True

def isDone(workflowID):
    sqlStr="""SELECT done FROM we_Workflow WHERE id='%s'
        """ %(str(workflowID))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if len(rows)==0:
       return False
    if rows[0][0]=='true':
         return True
    return False


def register(workflowID,parameters={}):
   """
   __register__
    
   register a workflow
   parameters:priority,request_type,prod_mgr_url

   if the workflow has already been registered it gives a warning and moves on.
   """
   descriptionMap={'priority':'priority','request_type':'workflow_type',\
       'prod_mgr_url':'prod_mgr_url','workflow_spec_file':'workflow_spec_file'}
   # check with attributes are provided.
   description=parameters.keys()

   # create values part
   sqlStrValues='('
   comma=False
   for attribute in description:
        if comma :
            sqlStrValues+=','
        elif not comma :
            comma=True
        sqlStrValues+=descriptionMap[attribute]

   sqlStrValues+=',id'
   sqlStrValues+=')'

   # build sql statement
   sqlStr="INSERT INTO we_Workflow"+sqlStrValues+" VALUES("
   valueComma=False
   for attribute in description:
       if valueComma:
           sqlStr+=','
       else:
           valueComma=True
       sqlStr+='"'+str(parameters[attribute])+'"'
   sqlStr+=',"'+str(workflowID)+'"'
   sqlStr+=')'
   sqlStr+=" ON DUPLICATE KEY UPDATE "
   comma=False
   for attribute in description:
       if comma: 
           sqlStr+=','
       elif not comma :
           comma=True
       sqlStr+=descriptionMap[attribute]+'="'+str(parameters[attribute])+'"'
   Session.execute(sqlStr)


def remove(workflowID=[]):
   """
   __remove__

   removes a (or multiple)  workflow entry (entries)
   """
   if(type(workflowID)!=list):
       workflowID=[str(workflowID)]
   if len(workflowID)==0:
       return
   if len(workflowID)==1:
       sqlStr="""DELETE FROM we_Workflow WHERE id="%s"
       """ %(str(workflowID[0]))
   else:
       sqlStr="""DELETE FROM we_Workflow WHERE id IN 
       %s """ %(str(tuple(workflowID)))
   Session.execute(sqlStr)

def setEventsProcessedIncrement(workflowID,eventsProcessed):
   sqlStr="""UPDATE we_Workflow SET events_processed=events_processed+%s WHERE 
   id="%s" """ %(str(eventsProcessed),str(workflowID))
   Session.execute(sqlStr)

def setFinished(workflowID=[]):
   if(type(workflowID)!=list):
       workflowID=[str(workflowID)]
   if len(workflowID)==0:
       return
   if len(workflowID)==1:
       sqlStr="""UPDATE we_Workflow SET done="true" WHERE id="%s"
       """ %(str(workflowID[0]))
   else:
       sqlStr="""UPDATE we_Workflow SET done="true" WHERE id IN 
       %s """ %(str(tuple(workflowID)))
   Session.execute(sqlStr)

def setWorkflowLocation(workflowID,workflowLocation):
   """
   __setWorkflowLocation__

   sets the (local) location of the workflow as downloaded.
   """    
   sqlStr="""UPDATE we_Workflow SET workflow_spec_file="%s" 
   WHERE id="%s" """ %(str(workflowLocation),str(workflowID))
   Session.execute(sqlStr)

