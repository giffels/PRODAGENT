#/usr/bin/env python

from ProdAgent.WorkflowEntities import Job
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session


def register(jobID,fileIDs=[]):
   """
   __register__

   registers a set of files associated to a jobid
   fileIDs is a array of dictionaries with a fileID (usually an lfn)
   an a number of events per file.

   if will only register this if the associated job is generated via the prodmgr. 
   This can be easily checked by looking at the allocation ID of a job.

   It will give a warning if it is not part of an allocation and moves on.

   """
   sqlStr="""INSERT INTO we_File(id,events_processed,job_id) VALUES"""
   comma=False
   for fileID in fileIDs:
       if comma:
           sqlStr+=','
       else:
           comma=True
       sqlStr+='("'+str(fileID['lfn'])+'","'+str(fileID['events'])+'","'+str(jobID)+'")'
   Session.execute(sqlStr)

def merged(fileIDs=[],failed=False):
   """
   __mergeSuccess_

   registers a merge when successful, also check if all files of the
   associated jobs have been merged. 

   It will give a warning if it is not part of an allocation and moves on.
   """
   if type(fileIDs)!=list:
      fileIDs=[fileIDs]
   if len(fileIDs)==1:
      sqlStr="""SELECT events_processed FROM we_File WHERE id="%s"
      """ %(str(fileIDs[0]))
   else:
      sqlStr="""SELECT SUM(events_processed),job_id FROM we_File WHERE id IN %s
      GROUP BY job_id
      """ %(str(tuple(fileIDs)))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   jobIDs=[]
   for row in rows:
       if failed:
           Job.setEventsProcessedIncrement(row[1],0)
       else: 
           Job.setEventsProcessedIncrement(row[1],row[0]) 
       jobIDs.append(row[1])
   if len(fileIDs)==1:
      sqlStr="""DELETE FROM we_File WHERE id="%s"
      """ %(str(fileIDs[0]))
   else:
      sqlStr="""DELETE FROM we_File WHERE id IN %s
      """ %(str(tuple(fileIDs)))
   Session.execute(sqlStr)
   if len(jobIDs)==1:
      sqlStr="""SELECT we_Job.id,we_File.id FROM we_Job LEFT JOIN (we_File) ON (we_Job.id=we_File.job_id)
      WHERE we_Job.id="%s" """ %(str(jobIDs[0]))
   else:
      sqlStr="""SELECT we_Job.id,we_File.id FROM we_Job LEFT JOIN (we_File) ON (we_Job.id=we_File.job_id)
      WHERE we_Job.id IN %s """ %(str(tuple(jobIDs)))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   jobIDs=[]
   for row in rows:
      if not row[1]:
           jobIDs.append(row[0]) 
   return jobIDs
   



   
