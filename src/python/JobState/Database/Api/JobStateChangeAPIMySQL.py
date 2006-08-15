#!/usr/bin/env python


from JobState.Database.Api import JobStateInfoAPIMySQL
from JobState.Database.Api.RacerException import RacerException
from JobState.Database.Api.RetryException import RetryException
from JobState.Database.Api.RunException import RunException
from JobState.Database.Api.SubmitException import SubmitException
from JobState.Database.Api.TransitionException import TransitionException
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect 


##########################################################################
# register method
##########################################################################

def register(jobSpecId, jobType, maxRetries, maxRacers = 1):   
       conn=connect(False)
       dbCur=conn.cursor()
       try:
           sqlStr="INSERT INTO js_JobSpec(JobSpecID,                   \
                   JobType,MaxRetries, MaxRacers,\
                   Retries,State) VALUES(\""+str(jobSpecId)+"\",             \
                                             \""+str(jobType)+"\",           \
                                             \""+str(maxRetries)+"\",   \
                                             \""+str(maxRacers)+"\",    \
                                             \"0\",                     \
                                             \"register\");"
           dbCur.execute("START TRANSACTION")
           try:
               dbCur.execute(sqlStr)
           except Exception,ex:
               raise ProdAgentException("Error registering job. You probably are trying to register a job using an job id/job name that has already been used for registration. Your job id/job name is: "+str(jobSpecId)+". Original error: "+str(ex[1]))
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise 

##########################################################################
# create method
##########################################################################
      
def create(jobSpecId, cacheDir): 
       conn=connect(False)
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
               raise TransitionException("Illegal state "+\
                     "transition: "+ state+ "-->create")
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

##########################################################################
# createFailure method
##########################################################################
def createFailure(jobSpecId):
       conn=connect(False)
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
                   raise TransitionException("Illegal state "+  
                         "transition: "+state+"-->createFailure")
               # now check if we need to raise a sumit exception as
               # we have reached the maximum number of retries:
               #check if we have not reach the maximum number of retries
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1) ):
                   raise RetryException("reached "+ \
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise SubmitException("SubmitFailure failed, please try again")
           dbCur.execute("COMMIT")
           dbCur.execute("START TRANSACTION")
           # now check if we need to raise a retry exception as
           # we have reached the maximum number of retries:
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
               raise RetryException("reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise


##########################################################################
# inProgress method
##########################################################################

def inProgress(jobSpecId): 
       conn=connect(False)
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
               raise TransitionException("Illegal state "+ \
                                         "transition: "+ state+ \
                                          "-->inProgress")
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

##########################################################################
# submit method
##########################################################################

def submit(jobSpecId):
       conn=connect(False)
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
                 raise TransitionException("Illegal state "+\
                                           "transition: "+state+ \
                                          "-->submit")
              #check if we have not reach the maximum number of retries
              if( (int(generalState['Retries'])+int(generalState['Racers']))> (int(generalState['MaxRetries'])-1)):
                 raise RetryException("reached "+
                                       "maximum number of retries "+
                                        str(generalState['MaxRetries'])+ \
                                       " (this includes running jobs)")
               #check if we have not reach the maximum number of simulatneous 
               #jobs 
              if(int(generalState['Racers'])> (int(generalState['MaxRacers'])-1)):
                  raise RacerException("job with id: "+str(jobSpecId)+
                                        " is already submitted will not resubmit")
              raise SubmitException("Submit failed, please try again")
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise
         
##########################################################################
# submitFailure  method
##########################################################################

def submitFailure(jobSpecId):
       conn=connect(False)
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
                   raise TransitionException("Illegal state "+  
                         "transition: "+state+"-->submitFailure")
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
                   raise RetryException("reached "+
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise SubmitException("SubmitFailure failed, please try again")

           dbCur.execute("COMMIT")

           dbCur.execute("START TRANSACTION")
           # now check if we need to raise a retry exception as
           # we have reached the maximum number of retries:
           generalState=JobStateInfoAPIMySQL.general(jobSpecId,dbCur)
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
               raise RetryException("reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise
         
##########################################################################
# runFailure method
##########################################################################

def runFailure(jobSpecId, jobInstanceId = None, runLocation = None, jobReportLocation = None):
       conn=connect(False)
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
                   raise TransitionException("Illegal state "+ \
                       "transition: "+state+ "-->runFailure")
               racers=int(generalState['Racers'])
               if(racers == 0):
                   raise RacerException("Negative number of racers, "+\
                       "is not possible, will not update ")
               if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)): 
                   raise RetryException("reached "+
                       "maximum number of retries "+
                       str(generalState['MaxRetries']))
               raise RunException("runFailure failed, please try again")

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
               raise RetryException("reached "+
                   "maximum number of retries "+
                   str(generalState['MaxRetries']))

           dbCur.execute("COMMIT")

           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

##########################################################################
# finished method
##########################################################################

def finished(jobSpecId): 
       conn=connect(False)
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
               raise TransitionException("Illegal state "+\
                  "transition: "+state+"-->finished")
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

##########################################################################
# cleanout method
##########################################################################

def cleanout(jobSpecId): 
       conn=connect(False)
       dbCur=conn.cursor()
       try:
           dbCur.execute("START TRANSACTION")
           sqlStr4="""DELETE FROM js_JobSpec WHERE 
                     JobSpecID="%s";""" %(jobSpecId)
           # not every mysql version supports cascade and foreign keys
           sqlStr3="""DELETE FROM js_JobInstance WHERE 
                      JobSpecID="%s";""" %(jobSpecId)
           sqlStr2="""DELETE FROM tr_Trigger WHERE 
                      JobSpecID="%s";""" %(jobSpecId)
           sqlStr1="""DELETE FROM tr_Action WHERE 
                      JobSpecID="%s";""" %(jobSpecId)
           dbCur.execute(sqlStr1)
           dbCur.execute(sqlStr2)
           dbCur.execute(sqlStr3)
           dbCur.execute(sqlStr4)
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

##########################################################################
# setRacer method
##########################################################################

def setRacer(jobSpecId,maxRacers):
       """
       Sets the maximum number of the same jobs that can be run at the
       same time (number of racers).
       """
       conn=connect(False)
       dbCur=conn.cursor()
       try:

           dbCur.execute("START TRANSACTION")
           sqlStr1="""UPDATE js_JobSpec SET MaxRacers="%s" WHERE
                      JobSpecID="%s"; """ %(str(maxRacers),str(jobSpecId)) 
           rowsModified=dbCur.execute(sqlStr1)
           if rowsModified!=1:
              raise ProdAgentException("This jobspec with ID "+\
                              str(jobSpecId)+" does not exist")
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
       except:
           dbCur.execute("ROLLBACK")
           dbCur.close()
           conn.close()
           raise

def purgeStates():
   conn=connect(False)
   dbCur=conn.cursor()
   try:

       dbCur.execute("START TRANSACTION")
       sqlStr1="""DELETE FROM js_JobSpec;"""
       sqlStr2="""DELETE FROM js_JobInstance;"""
       sqlStr3="""DELETE FROM tr_Trigger;"""
       sqlStr4="""DELETE FROM tr_Action;"""
       dbCur.execute(sqlStr1)
       # if cascacding is suported the next
       # queries are not needed.
       dbCur.execute(sqlStr2)
       dbCur.execute(sqlStr3)
       dbCur.execute(sqlStr4)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
   except:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise

def startedJobs(daysBack):
   conn=connect(False)
   dbCur=conn.cursor()

   now=datetime.datetime.now()
   delta=datetime.timedelta(days=int(daysBack))
   daysBack=now-delta
   try:
       dbCur.execute("START TRANSACTION")
       sqlStr='SELECT JobSpecID from js_JobSpec WHERE Time<"'+str(daysBack)+'";'
       # NOTE:this can be potentially large, but we assume it will be not larger than
       # NOTE: several mbs.
       dbCur.execute(sqlStr)
       result=dbCur.fetchall() 
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
       return result
   except:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise
