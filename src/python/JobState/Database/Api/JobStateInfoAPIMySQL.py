#!/usr/bin/env python
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
           raise Exception("ERROR:", "Job with JobID "+str(JobSpecId)+ \
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
           raise Exception("ERROR:", "Job with JobID "+str(JobSpecId)+ \
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
