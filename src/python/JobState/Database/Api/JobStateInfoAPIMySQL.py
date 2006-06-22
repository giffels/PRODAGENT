#!/usr/bin/env python

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect

def general(JobSpecId,dbCur = None):
       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")

       sqlStr='SELECT JobType,MaxRetries,Retries, '+\
              'State,CacheDirLocation, MaxRacers, Racers '+ \
              'FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       dbCur.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=dbCur.fetchall()
       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
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

def isRegistered(JobSpecId,dbCur = None):
       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")

       sqlStr='SELECT JobType FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       dbCur.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=dbCur.fetchall()
       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
       if len(rows)==0:
           return False
       return True

def lastLocations(JobSpecId,dbCur = None):
       sqlStr='SELECT Location from js_JobInstance WHERE JobSpecID="'+\
               JobSpecId+'";'

       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")

       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       if len(rows)==0:
           raise ProdAgentException("Job with JobID "+str(JobSpecId)+ \
                           " has no jobs running yet")
       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
       result=[]
       for i in rows:
           result.append(i[0])
       return result
          
def jobReports(JobSpecId, dbCur = None):
       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
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

       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
       return result

def jobSpecTotal(dbCur = None):
       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       sqlStr='SELECT COUNT(JobSpecID) FROM js_JobSpec;'
       dbCur.execute(sqlStr)
       #this query will not return many (= thousands) of results.
       rows=dbCur.fetchall()
       result=rows[0][0]
       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
       return result

def rangeGeneral(start = -1 , nr = -1 ,dbCur = None):
       if(dbCur==None):
           conn=connect()
           dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")

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
       dbCur.execute("COMMIT")
       if(dbCur==None):
          dbCur.close()
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
