#!/usr/bin/env python
import datetime

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect

def general(JobSpecId,dbCur1 = None):
   try:
       #NOTE we should put this in separte commit,rollback and connect methods
       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1

       sqlStr='SELECT JobType,MaxRetries,Retries, '+\
              'State,CacheDirLocation, MaxRacers, Racers '+ \
              'FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       dbCur.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=dbCur.fetchall()
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       if len(rows)==0:
           raise ProdAgentException("Job with JobID "+str(JobSpecId)+ \
                           " does not exists")
       # format it in a dictionary
       return {'JobType':rows[0][0], \
               'MaxRetries':rows[0][1], \
               'Retries':rows[0][2], \
               'State':rows[0][3], \
               'CacheDirLocation':rows[0][4], \
               'MaxRacers':rows[0][5], \
               'Racers':rows[0][6] \
              }
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise


def isRegistered(JobSpecId,dbCur1 = None):
   try:
       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1

       sqlStr='SELECT JobType FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       dbCur.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=dbCur.fetchall()
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       if len(rows)==0:
           return False
       return True
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise

def lastLocations(JobSpecId,dbCur1 = None):
   try:
       sqlStr='SELECT Location from js_JobInstance WHERE JobSpecID="'+\
               JobSpecId+'";'

       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1

       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       if len(rows)==0:
           raise ProdAgentException("Job with JobID "+str(JobSpecId)+ \
                           " has no jobs running yet")
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       result=[]
       for i in rows:
           result.append(i[0])
       return result
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise
          
def jobReports(JobSpecId, dbCur1 = None):
   try:
       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1
       sqlStr='SELECT JobReportLocation FROM js_JobInstance WHERE '+ \
              'JobSpecID="'+JobSpecId+'" AND JobReportLocation<>"NULL";'
       dbCur.execute(sqlStr)
       #this query will not return many (= thousands) of results.
       rows=dbCur.fetchall()

       result=[]
       #convert to an array:
       #NOTE: can this be done more efficient?
       for i in rows:
          result.append(i[0])

       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       return result
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise

def jobSpecTotal(dbCur1 = None):
   try:
       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1
       sqlStr='SELECT COUNT(JobSpecID) FROM js_JobSpec;'
       dbCur.execute(sqlStr)
       #this query will not return many (= thousands) of results.
       rows=dbCur.fetchall()
       result=rows[0][0]
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       return result
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise

def rangeGeneral(start = -1 , nr = -1 ,dbCur1 = None):
   try:
       if(dbCur1==None):
           conn=connect(False)
           dbCur=conn.cursor()
           dbCur.execute("START TRANSACTION")
       else:
           dbCur=dbCur1

       if ( (start == -1) and (nr == -1) ):
           start=0
           nr=jobSpecTotal()
       if start<0:
           raise ProdAgentException('Start should be larger than 0!')
       elif nr<0:
           raise ProdAgentException('Number should be larger than 0!')

       sqlStr='SELECT JobSpecID, JobType,MaxRetries,Retries, '+\
              'State,CacheDirLocation, MaxRacers, Racers '+ \
              'FROM js_JobSpec LIMIT '+str(start)+','+str(nr)+';'
       dbCur.execute(sqlStr)
       #NOTE: we kind off assume that we only deal with small subsets.
       rows=dbCur.fetchall()
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       result=[]
       resultDescription=['JobSpecID','JobType','MaxRetries','Retries','State','CacheDirLocation','MaxRacers','Racers']
       result.append(resultDescription)

       #NOTE: we change it from tuples to arrays to avoid problems with 
       #NOTE: XMLRPC calls in the future.
       #NOTE: is there a easier way of doing this?
       for row in rows:
           resultRow=[]
           for entry in row:
               resultRow.append(entry)
           result.append(resultRow)
       return result    
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise

def startedJobs(daysBack,dbCur1 = None):
   if(dbCur1==None):
       conn=connect(False)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
   else:
       dbCur=dbCur1

   now=datetime.datetime.now()
   delta=datetime.timedelta(days=int(daysBack))
   daysBack=now-delta
   try:
       sqlStr='SELECT JobSpecID from js_JobSpec WHERE Time<"'+str(daysBack)+'";'
       # NOTE:this can be potentially large, but we assume it will be not larger than
       # NOTE: several mbs.
       dbCur.execute(sqlStr)
       result=dbCur.fetchall()
       if dbCur1==None:
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       return result
   except:
       if dbCur1==None:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
       raise

