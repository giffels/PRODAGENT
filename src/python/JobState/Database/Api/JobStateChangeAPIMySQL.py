#!/usr/bin/env python
from JobState.Database.Api import JobStateInfoAPIMySQL
from JobState.Database.Api.RetryException import RetryException
from JobState.Database.Api.RacerException import RacerException
from JobState.Database.Api.TransitionException import TransitionException
from ProdAgentDB.Connect import connect 


##########################################################################
# register method
##########################################################################

def register(jobSpecId, jobType, maxRetries, maxRacers = 1):   
       conn=connect()
       dbCur=conn.cursor()
       try:
           sqlStr="INSERT INTO js_JobSpec(JobSpecID,                   \
                   JobType,MaxRetries, MaxRacers,\
                   Retries,State) VALUES(\""+jobSpecId+"\",             \
                                             \""+jobType+"\",           \
                                             \""+str(maxRetries)+"\",   \
                                             \""+str(maxRacers)+"\",    \
                                             \"0\",                     \
                                             \"register\");"
           dbCur.execute("START TRANSACTION")
           dbCur.execute(sqlStr)
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise 

##########################################################################
# create method
##########################################################################
      
def create(jobSpecId, cacheDir): 
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr="UPDATE js_JobSpec SET State=\"create\",\
                   CacheDirLocation=\""+cacheDir+"\" \
                   WHERE JobSpecID=\""+jobSpecId+"\" AND State=\"register\";"
 
           rowsModified=dbCur.execute(sqlStr)
           if (rowsModified!=1):
               try:
                  state=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)['State']
               except:
                  state="Undefined"
               raise TransitionException("ERROR:", "Illegal state "+\
                     "transition: "+ state+ "-->create")
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

##########################################################################
# createFailure method
##########################################################################
def createFailure(jobSpecId):
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr="UPDATE js_JobSpec SET Retries=Retries+1 WHERE "+ \
                  " JobSpecID=\""+ jobSpecId +"\" AND "+ \
                  " Retries<MaxRetries AND State=\"register\";"
           rowsModified=dbCur.execute(sqlStr)
           if rowsModified!=1:
               try:
                   generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
                   state=generalState['State']
               except:
                   state="Undefined"
               if  not state in ['create']:
                   raise TransitionException("ERROR:", "Illegal state "+  
                         "transition: "+state+"-->createFailure")
               # now check if we need to raise a sumit exception as
               # we have reached the maximum number of retries:
               #check if we have not reach the maximum number of retries
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1) ):
                   raise RetryException("ERROR:", "reached "+ \
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise Exception("ERROR","SubmitFailure failed, please try again")
           dbCur.execute("COMMIT")
           dbCur.execute("START TRANSACTION")
           # now check if we need to raise a retry exception as
           # we have reached the maximum number of retries:
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
               raise RetryException("ERROR:", "reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise


##########################################################################
# inProgress method
##########################################################################

def inProgress(jobSpecId): 
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr="UPDATE js_JobSpec SET State=\"inProgress\"\
                   WHERE JobSpecID=\""+jobSpecId+"\" AND State=\"create\";"
           rowsModified=dbCur.execute(sqlStr)
           if rowsModified!=1:
               try:
                   state=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)['State']
               except:
                   state="Undefined"
               raise TransitionException("ERROR:", "Illegal state "+ \
                                         "transition: "+ state+ \
                                          "-->inProgress")
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

##########################################################################
# submit method
##########################################################################

def submit(jobSpecId):
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr="UPDATE js_JobSpec SET "+    \
                  "Racers=Racers+1 WHERE JobSpecID=\""+str(jobSpecId)+ \
                  "\" AND (Racers+Retries)<MaxRetries AND "+ \
                  "Racers<MaxRacers AND State=\"inProgress\";"
           rowsModified=dbCur.execute(sqlStr)
           if rowsModified!=1:
              try:
                 generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
                 state=generalState['State']
              except:
                 state="Undefined"
              if  not state in ['inProgress']:
                 raise TransitionException("ERROR:", "Illegal state "+\
                                           "transition: "+state+ \
                                          "-->submit")
              #check if we have not reach the maximum number of retries
              if( (int(generalState['Retries'])+int(generalState['Racers']))> (int(generalState['MaxRetries'])-1)):
                 raise RetryException("ERROR:", "reached "+
                                       "maximum number of retries "+
                                        str(generalState['MaxRetries'])+ \
                                       " (this includes running jobs)")
               #check if we have not reach the maximum number of simulatneous 
               #jobs 
              if(int(generalState['Racers'])> (int(generalState['MaxRacers'])-1)):
                  raise RacerException("ERROR:", "job with id: "+str(jobSpecId)+
                                        " is already submitted will not resubmit")
              raise Exception("ERROR","Submit failed, please try again")
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise
         
##########################################################################
# submitFailure  method
##########################################################################

def submitFailure(jobSpecId):
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr="UPDATE js_JobSpec SET Retries=Retries+1 "+\
                  " WHERE JobSpecID=\""+ \
           jobSpecId+"\" AND Retries<MaxRetries AND State=\"inProgress\";"
           rowsModified=dbCur.execute(sqlStr)
           if rowsModified!=1:
               try:
                   generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
                   state=generalState['State']
               except:
                   state="Undefined"
               if  not state in ['inProgress']:
                   raise TransitionException("ERROR:", "Illegal state "+  
                         "transition: "+state+"-->submitFailure")
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
                   raise RetryException("ERROR:", "reached "+
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise Exception("ERROR","SubmitFailure failed, please try again")

           dbCur.execute("COMMIT")

           dbCur.execute("START TRANSACTION")
           # now check if we need to raise a retry exception as
           # we have reached the maximum number of retries:
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
               raise RetryException("ERROR:", "reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise
         
##########################################################################
# runFailure method
##########################################################################

def runFailure(jobSpecId, jobInstanceId = None, runLocation = None, jobReportLocation = None):
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           sqlStr="UPDATE js_JobSpec SET Retries=Retries+1,Racers=Racers-1 "+\
                  "WHERE JobSpecID=\""+ jobSpecId+"\" AND "+\
                  "Racers>0 AND Retries<MaxRetries AND State=\"inProgress\";"
           rowsModified=dbCur.execute(sqlStr)
           if rowsModified!=1:
               try:
                   generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
                   state=generalState['State']
               except:
                   state="Undefined"
               if(state!='inProgress'):
                   raise TransitionException("ERROR:", "Illegal state "+ \
                       "transition: "+state+ "-->runFailure")
               racers=int(generalState['Racers'])
               if(racers == 0):
                   raise Exception("ERROR","Negative number of racers, "+\
                       "is not possible, will not update ")
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
                   raise RetryException("ERROR:", "reached "+
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise Exception("ERROR","runFailure failed, please try again")

           # NOTE: we make these exceptions as we are waiting to 
           # extract some of this information from the job report.
           if(jobReportLocation!=None) and \
             (runLocation!=None) and       \
             (jobInstanceId!=None):
               sqlStr2="""INSERT INTO js_JobInstance(JobReportLocation,
                       Location,JobSpecID,JobInstanceID) 
                       VALUES("%s","%s","%s","%s") ;""" %(jobReportLocation,runLocation,jobSpecId,jobInstanceId)
           elif (jobReportLocation==None) and  \
                (runLocation!=None) and  \
                (jobInstanceId!=None):
               sqlStr2="""INSERT INTO js_JobInstance(Location,JobSpecID,
                   JobInstanceID) VALUES("%s","%s","%s") ;""" %(runLocation,jobSpecId,jobInstanceId)
           elif (jobReportLocation!=None) and  \
                (runLocation==None) and  \
                (jobInstanceId==None):
               sqlStr2="""INSERT INTO js_JobInstance(JobReportLocation,
                   JobSpecID) VALUES("%s","%s") ;""" %(jobReportLocation,jobSpecId)
           dbCur.execute(sqlStr2)
           dbCur.execute("COMMIT")
           dbCur.execute("START TRANSACTION")
           # now check if we need to raise a sumit exception as
           # we have reached the maximum number of retries:
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
               raise RetryException("ERROR:", "reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")

           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

##########################################################################
# finished method
##########################################################################

def finished(jobSpecId): 
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr1="""UPDATE js_JobSpec SET State="finished" WHERE 
                     JobSpecID="%s" AND State="inProgress";""" %(jobSpecId)
           rowsModified=dbCur.execute(sqlStr1)
           if rowsModified!=1:
               try:
                  state=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)['State']
               except:
                  state="Undefined"
               raise TransitionException("ERROR:", "Illegal state "+\
                  "transition: "+state+"-->finished")
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

##########################################################################
# cleanout method
##########################################################################

def cleanout(jobSpecId): 
       conn=connect()
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr1="""DELETE FROM js_JobSpec WHERE 
                     JobSpecID="%s";""" %(jobSpecId)
           # not every mysql version supports cascade and foreign keys
           sqlStr2="""DELETE FROM js_JobInstance WHERE 
                      JobSpecID="%s";""" %(jobSpecId)
           rowsModified=dbCur.execute(sqlStr1)
           dbCur.execute(sqlStr2)
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

##########################################################################
# setRacer method
##########################################################################

def setRacer(jobSpecId,maxRacers):
       """
       Sets the maximum number of the same jobs that can be run at the
       same time (number of racers).
       """
       conn=connect()
       dbCur=conn.cursor()
       try:

           dbCur.execute("START TRANSACTION")
           sqlStr1="""UPDATE js_JobSpec SET MaxRacers="%s" WHERE
                      JobSpecID="%s"; """ %(str(maxRacers),str(jobSpecId)) 
           rowsModified=dbCur.execute(sqlStr1)
           if rowsModified!=1:
              raise Exception("ERROR","This jobspec with ID "+\
                              str(jobSpecId)+" does not exist")
           dbCur.execute("COMMIT")
           dbCur.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           raise

