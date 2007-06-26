#/usr/bin/env python

import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session

from ProdAgent.Core.Codes import exceptions 
from ProdAgent.WorkflowEntities import Aux

import ProdAgent.WorkflowEntities.Allocation as Allocation
import ProdAgent.WorkflowEntities.Workflow as Workflow

def amount():
   """
   __amount__
   """
   sqlStr="""SELECT COUNT(*) FROM we_Job"""
   Session.execute(sqlStr)
   rows=Session.fetchall()
   return rows[0][0]

def exists(jobID):
   """
   __exists__
   
   returns true if the job exists
   """
   sqlStr="""SELECT COUNT(*) FROM we_Job WHERE id="%s"
   """ %(str(jobID))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if rows[0][0]==1:
      return True
   return False
  
def get(jobID=[]):
   """
   __get__

   returns job information as registered in the database
   """
   if(type(jobID)!=list):
       jobID=[str(jobID)]
   if len(jobID)==0:
       return
   if len(jobID)==1:
       sqlStr="""SELECT allocation_id,cache_dir,events_processed,id,job_spec_file,job_type,
       max_retries,max_racers,retries,racers,status,Time,workflow_id, owner FROM we_Job WHERE id="%s" """ %(str(jobID[0]))
   else:
       sqlStr="""SELECT allocation_id,cache_dir,events_processed,id,job_spec_file,job_type,
       max_retries,max_racers,retries,racers,status,Time,workflow_id, owner FROM we_Job WHERE id IN %s """ %(str(tuple(jobID)))
   Session.execute(sqlStr)
   description=['allocation_id','cache_dir','events_processed','id','job_spec_file','job_type','max_retries','max_racers','retries','racers','status','time_stamp','workflow_id', 'owner']
   result=Session.convert(description,Session.fetchall())
   if len(result)==0:
      return None
   if len(result)==1:
      return result[0]
   return result

#NOT TESTED
def getRange(start=0,nr=0):
   """
   __getRange__

   returns job information for a particular range
   """
   sqlStr="""SELECT allocation_id,cache_dir,events_processed,id,job_spec_file,job_type,
   max_retries,max_racers,retries,racers,status,Time,workflow_id, owner FROM we_Job LIMIT %s,%s
   """ %(start,nr) 
   Session.execute(sqlStr)
   description=['allocation_id','cache_dir','events_processed','id','job_spec_file','job_type','max_retries','max_racers','retries','racers','status','time_stamp','workflow_id','owner']
   return Session.convert(description,Session.fetchall())

def register(workflowID=None,allocationID=None,job={}):
   """
   __register__
   
   registers a set of jobs associated to an allocationID
   jobs is an array of dictionaries with keys: jobID,jobSpecLocation,jobType,maxRetries,
   maxRacers.

   If the job is already registered (as might be the case when it is a prodmgr based
   job it will just update it.

   """
   descriptionMap={'id':'id','spec':'job_spec_file',\
       'job_type':'job_type','max_retries':'max_retries',\
       'max_racers':'max_racers','owner':'owner'}
   # check if there is any input
   if not job:
      return
   if len(job)==0:
      return
   if type(job)==dict:
      jobs=[job]
   else:
      jobs=job
   # check with attributes are provided.
   for job in jobs:
       description=job.keys()

       # create values part
       sqlStrValues='('
       comma=False
       for attribute in description:
            if comma :
                sqlStrValues+=','
            elif not comma :
                comma=True
            sqlStrValues+=descriptionMap[attribute]
    
       if workflowID: 
            sqlStrValues+=',workflow_id'
       if allocationID: 
            sqlStrValues+=',allocation_id'
       sqlStrValues+=')'
     
       # build sql statement
       sqlStr="INSERT INTO we_Job"+sqlStrValues+" VALUES("
       valueComma=False
       for attribute in description:
           if valueComma:
               sqlStr+=','
           else:
               valueComma=True
           sqlStr+='"'+str(job[attribute])+'"'
       if workflowID: 
            sqlStr+=',"'+str(workflowID)+'"'
       if allocationID: 
            sqlStr+=',"'+str(allocationID)+'"'
       sqlStr+=')'
       sqlStr+=" ON DUPLICATE KEY UPDATE "
       comma=False
       for attribute in description:
           if comma and attribute!='jobID':
               sqlStr+=','
           elif not comma and attribute!='jobID':
               comma=True
           if attribute!='jobID':
               sqlStr+=descriptionMap[attribute]+'="'+str(job[attribute])+'"'
       Session.execute(sqlStr)
       
def remove(jobIDs=[]):
   """
   __remove__

   removes the jobs with the specified ids.
   """
   Aux.removeJob(jobIDs)

def removeAll():
   sqlStr1="""DELETE FROM we_Job;"""
   sqlStr2="""DELETE FROM tr_Trigger;"""
   sqlStr3="""DELETE FROM tr_Action;"""
   Session.execute(sqlStr1)
   #Session.execute(sqlStr2)
   #Session.execute(sqlStr3)

       
def registerFailure(jobID,failureState,parameters={}):
   """
   __registerFailure__

   registers a failure of a job and take appropiate actions.
   """
   if(failureState=='create'):
       sqlStr="""UPDATE we_Job SET retries=retries+1 WHERE
       id='%s' AND retries<max_retries AND status='register'
       """ %(str(jobID))
   elif(failureState=='submit'):
       sqlStr="""UPDATE we_Job SET retries=retries+1 WHERE
       id='%s' AND retries<max_retries AND status='in_progress'
       """ %(str(jobID))
   elif(failureState=='run'):
       sqlStr="""UPDATE we_Job SET retries=retries+1,racers=racers-1 WHERE
       id='%s' AND racers>0 AND retries<max_retries AND status='in_progress'
       """ %(str(jobID))
   rowsModified=Session.execute(sqlStr)
   jobDetails=get(jobID)
   if not jobDetails:
       raise ProdException(exceptions[3012]+'undefined',3012) 
   if(int(jobDetails['retries'])>(int(jobDetails['max_retries'])-1) ):
       raise ProdException(exceptions[3013]+str(jobDetails['MaxRetries']),3013)

   if failureState!='run' and rowsModified!=1:
       raise ProdException(exceptions[3014],3014)

   if failureState=='run' and rowsModified!=1:
       if jobDetails['status']!='in_progress':
           raise ProdException(exceptions[3018]+str(jobDetails['status']),3018)
       if int(jobDetails['racers'])==0:
           raise ProdException(exceptions[3016]+str(jobDetails['status']),3016)
       raise ProdException(exceptions[3017]+str(generalState['MaxRetries']),3017)

def setCacheDir(jobID,cacheDir):
   """
   __setCacheDir__
   sets the job cache dir of a particular job.
   """
   sqlStr="""UPDATE we_Job SET cache_dir="%s" WHERE
   id="%s" """ %(str(cacheDir),str(jobID))
   Session.execute(sqlStr)

def setMaxRacers(jobIDs=[],maxRacers=1):
   """
   __setMaxRacers__
   (re)sets the maximum number of racers.
   """
   if maxRacers<1:
       raise ProdException(exceptions[3005],3005)
   if(type(jobIDs)!=list):
       jobIDs=[str(jobIDs)]
   if len(jobIDs)==0:
       return
   if len(jobIDs)==1:
       sqlStr="""UPDATE we_Job SET max_racers="%s" WHERE
       id="%s" """ %(str(maxRacers),str(jobIDs[0]))
   else:
       sqlStr="""UPDATE we_Job SET max_racers="%s" WHERE
       id IN %s """ %(str(maxRacers),str(tuple(jobIDs)))
   Session.execute(sqlStr)


def setMaxRetries(jobIDs=[],maxRetries=1):
   """
   __setMaxRetries__

   (re)sets the maximum retries
   """
   if maxRetries<1:
       raise ProdException(exceptions[3006],3006)
   if(type(jobIDs)!=list):
       jobIDs=[str(jobIDs)]
   if len(jobIDs)==0:
       return
   if len(jobIDs)==1:
       sqlStr="""UPDATE we_Job SET max_retries="%s" WHERE
       id="%s" """ %(str(maxRetries),str(jobIDs[0]))
   else:
       sqlStr="""UPDATE we_Job SET max_retries="%s" WHERE
       id IN %s """ %(str(maxRetries),str(tuple(jobIDs)))
   Session.execute(sqlStr)
   pass

def setState(jobID,state,parameters={}):
   """
   __setState__

   sets the state of a job (inProgress,submitted,finished)
   """
   sqlSetStr=''
   if len(parameters)>0:
      for parameter in parameters.keys():
          sqlSetStr+=','+parameter+'='+parameters[parameter]

   if state=="create":
       sqlStr="""UPDATE we_Job SET status="create" """
       if sqlSetStr!='':
            sqlStr+=sqlSetStr
       sqlStr+=""" WHERE id="%s" AND status="register" """ %(str(jobID))
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
            job=get(jobID)
            if not job:
                raise ProdException(exceptions[3009]+':'+str(jobID),3009) 
            raise ProdException(exceptions[3007]+':'+str(jobID),3007)
   elif state=="inProgress":
       sqlStr="""UPDATE we_Job SET status="in_progress" """
       if sqlSetStr!='':
            sqlStr+=sqlSetStr
       sqlStr+=""" WHERE id="%s" AND status="create" """ %(str(jobID)) 
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
            job=get(jobID)
            if not job:
                raise ProdException(exceptions[3009]+':'+str(jobID),3009) 
            raise ProdException(exceptions[3008]+':'+str(jobID),3008)
   elif state=="submit":
       sqlStr="""UPDATE we_Job SET racers=racers+1 """
       if sqlSetStr!='':
            sqlStr+=sqlSetStr
       sqlStr+=""" WHERE id="%s" AND racers+retries<max_retries AND 
       racers<max_racers AND status="in_progress" """ %(str(jobID))
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
            job=get(jobID)
            if not job:
                raise ProdException(exceptions[3009]+':'+str(jobID),3009) 
            if not job['status']=='in_progress':
                raise ProdException(exceptions[3010]+':'+str(jobID),3010) 
            if (job['retries']+job['racers']>(job['max_racers']-1)):
                raise ProdException(exceptions[3011]+':'+str(jobID),3011)
            if (job['racers']>(job['max_racers']-1)):
                raise ProdException(exceptions[3011]+':'+str(jobID),3011) 
   elif state=="finished": 
       sqlStr="""UPDATE we_Job SET status='finished' """
       if sqlSetStr!='':
            sqlStr+=sqlSetStr
       sqlStr+="""WHERE id='%s' AND status='in_progress' """ %(str(jobID))
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
            job=get(jobID)
            if not job:
                raise ProdException(exceptions[3009]+':'+str(jobID),3009) 
            raise ProdException(exceptions[3010]+':'+str(jobID),3010)

def setEventsProcessedIncrement(jobID,eventsProcessed=0):
   """
   __setEventsProcessedIncrement__

   increases the number of events processed
   """
   sqlStr="""UPDATE we_Job SET events_processed=events_processed+%s WHERE 
   id="%s" """ %(str(eventsProcessed),str(jobID))
   Session.execute(sqlStr)
   job=get(jobID)
   if job:
      if job['allocation_id']:
            Allocation.setEventsProcessedIncrement(job['allocation_id'],eventsProcessed)
      elif job['workflow_id']:
            Workflow.setEventsProcessedIncrement(job['workflow_id'],eventsProcessed)

