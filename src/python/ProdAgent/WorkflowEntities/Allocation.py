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
       sqlStr="""SELECT id,events_processed,details,prod_mgr_url,workflow_id,allocation_spec_file
       FROM we_Allocation WHERE id="%s" """ %(str(allocationID[0]))
   else:
       sqlStr="""SELECT id,events_processed,details,prod_mgr_url,workflow_id,allocation_spec_file
       FROM we_Allocation WHERE id IN %s """ %(str(tuple(allocationID)))
   Session.execute(sqlStr)
   description=['id','events_processed','details','prod_mgr_url','workflow_id','allocation_spec_file']
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

def isJobsFinished(allocationID):
   """
   __isJobsFinished__
   
   returns true if all jobs are finished for this allocation.
   """
   sqlStr="""SELECT COUNT(*) FROM we_Job WHERE allocation_id="%s"
   """ %(str(allocationID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if int(rows[0][0])==0:
       return True
   return False

def register(workflowID,allocations=[]):
   """
   __register__
   
   registers an allocation.

   allocations is an array of dictionarys containing keys:
   allocationID,prodMgrURL,allocationDetails
   """

   sqlStr="""INSERT INTO we_Allocation(id,prod_mgr_url,details,workflow_id) VALUES"""
   comma=False
   for allocation in allocations:
       if comma:
           sqlStr+=','
       else:
           comma=True
       sqlStr+='("'+allocation['id']+'",'
       sqlStr+='"'+allocation['prod_mgr_url']+'",'
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

def setAllocationSpecFile(allocationID,spec_file):
   sqlStr="""UPDATE we_Allocation SET allocation_spec_file="%s"
   WHERE id="%s" """ %(str(spec_file),str(allocationID))
   Session.execute(sqlStr)
   



