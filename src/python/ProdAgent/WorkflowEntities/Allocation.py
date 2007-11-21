#/usr/bin/env python
import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session

from ProdAgent.WorkflowEntities import Aux
from ProdAgent.WorkflowEntities import Workflow

def convertJobID(jobID):
   """
   __converts the jobid to the associated allocation id
   """
   cuts=jobID.split('_')
   if len(cuts)<4:
       return None 
   allocationId=''
   for i in xrange(0,4):
       allocationId+=cuts[i]
       if i!=3:
           allocationId+='_'
   return allocationId


def get(allocationID=[]):
   """
   __get__

   returns the allocations associated to particular ID

   """
   if(type(allocationID)!=list):
       allocationID=[str(allocationID)]
   if len(allocationID)==0:
       return
   if len(allocationID)==1:
       sqlStr="""SELECT id,events_missed,events_allocated,events_missed_cumul,events_processed,details,prod_mgr_url,workflow_id,allocation_spec_file
       FROM we_Allocation WHERE id="%s" """ %(str(allocationID[0]))
   else:
       sqlStr="""SELECT id,events_missed,events_allocated,events_missed_cumul,events_processed,details,prod_mgr_url,workflow_id,allocation_spec_file
       FROM we_Allocation WHERE id IN %s """ %(str(tuple(allocationID)))
   Session.execute(sqlStr)
   description=['id','events_missed','events_allocated','events_missed_cumul','events_processed','details','prod_mgr_url','workflow_id','allocation_spec_file']
   result=Session.convert(description,Session.fetchall(),oneItem=False,decode=['details'])
   if len(result)==0:
      return None
   if len(result)==1:
      return result[0]
   return result

def getEventsProcessed(allocationID=[]):
   """
   __getEventsProcessed__

   returns the events processed sofar for a particular allocation
   """
   if(type(allocationID)!=list):
       allocationID=[str(allocationID)]
   if len(allocationID)==0:
       return
   if len(allocationID)==1:
       sqlStr="""SELECT events_processed,id
       FROM we_Allocation WHERE id="%s" """ %(str(allocationID[0]))
   else:
       sqlStr="""SELECT events_processed,id
       FROM we_Allocation WHERE id IN %s """ %(str(tuple(allocationID)))
   description=['id','events_processed'] 
   result=Session.convert(description,Session.fetchall())
   if len(result)==0:
      return None
   if len(result)==1:
      return result[0]
   return result

def hasMissingEvents():
   sqlStr= """
SELECT we_Allocation.id,we_Allocation.events_missed,we_Allocation.events_missed_cumul,we_Allocation.events_processed,we_Allocation.details,we_Allocation.prod_mgr_url,we_Allocation.workflow_id,we_Allocation.allocation_spec_file FROM we_Allocation WHERE we_Allocation.id NOT IN (SELECT DISTINCT allocation_id FROM we_Job) AND events_missed > 0;
   """
   Session.execute(sqlStr)
   description=['id','events_missed','events_missed_cumul','events_processed','details','prod_mgr_url','workflow_id','allocation_spec_file']
   result=Session.convert(description,Session.fetchall(),oneItem=False,decode=['details'])
   if len(result)==0:
      return []
   return result

def isJobsFinished(allocationID):
   """
   __isJobsFinished__
   
   returns true if all jobs are finished for this allocation.
   """
   sqlStr="""
SELECT COUNT(*) FROM we_Job WHERE allocation_id="%s"
AND status<>'reallyFinished'
   """ %(str(allocationID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   allocation = get(allocationID)
   if (int(allocation['events_allocated']) <= 0):
      return True
   condition1 = float(allocation['events_missed'])/float(allocation['events_allocated'])  
   condition2 = float(allocation['events_missed_cumul'])/float(allocation['events_allocated'])
   condition3 = int(rows[0][0])
   logging.debug('Checking condition: '+str(condition1)+' '+str(condition2)+' '+str(condition3))
   if ( ( (condition1 < 0.01) or (condition2 > 3) ) and condition3 == 0):
   #if (condition3 == 0):
      logging.debug('Jobs for allocation '+str(allocationID)+ ' finished')
      return True
   return False

def register(workflowID,allocations=[]):
   """
   __register__
   
   registers an allocation.

   allocations is an array of dictionarys containing keys:
   allocationID,prodMgrURL,allocationDetails
   """

   sqlStr="""INSERT INTO we_Allocation(id,prod_mgr_url,events_allocated,details,workflow_id) VALUES"""
   comma=False
   for allocation in allocations:
       if comma:
           sqlStr+=','
       else:
           comma=True
       sqlStr+='("'+allocation['id']+'",'
       sqlStr+='"'+allocation['prod_mgr_url']+'",'
       sqlStr+='"'+str(allocation['events_allocated'])+'",'
       sqlStr+='"'+base64.encodestring(cPickle.dumps(allocation['details']))+'","'+str(workflowID)+'")'
   if len(allocations)>0:
       Session.execute(sqlStr)

def remove(allocationID=[]):
   """ 
   __remove__
   removes allocations with a particular ID
   """
   Aux.removeAllocation(allocationID)

def setEventsProcessedIncrement(allocationID,eventsProcessed=0):
   """
   
   """
   sqlStr="""UPDATE we_Allocation SET events_processed=events_processed+%s WHERE 
   id="%s" """ %(str(eventsProcessed),str(allocationID))
   Session.execute(sqlStr)
   allocation=get(allocationID)
   if allocation:
      if allocation['workflow_id']:
          Workflow.setEventsProcessedIncrement(allocation['workflow_id'],eventsProcessed)

def setEventsMissedIncrement(allocationID,eventsMissed=0):
   """
   
   """
   sqlStr="""UPDATE we_Allocation SET events_missed=events_missed+%s,
    events_missed_cumul=events_missed_cumul+%s WHERE 
   id="%s" """ %(str(eventsMissed),str(eventsMissed),str(allocationID))
   Session.execute(sqlStr)

def setEventsMissed(allocationID,eventsMissed=0):
   """
   
   """
   sqlStr="""UPDATE we_Allocation SET events_missed=%s WHERE 
   id="%s" """ %(str(eventsMissed),str(allocationID))
   Session.execute(sqlStr)

def setAllocationSpecFile(allocationID,spec_file):
   sqlStr="""UPDATE we_Allocation SET allocation_spec_file="%s"
   WHERE id="%s" """ %(str(spec_file),str(allocationID))
   Session.execute(sqlStr)
   
