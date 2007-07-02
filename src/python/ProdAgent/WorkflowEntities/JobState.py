#!/usr/bin/env python
from ProdAgent.Core.Codes import exceptions
from ProdCommon.Core.GlobalRegistry import GlobalRegistry
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Database import Session
from ProdCommon.Core.ProdException import ProdException

from ProdAgent.WorkflowEntities import Job

def register(jobSpecId, jobType, maxRetries, maxRacers,workflowID=' '):
       """
       _register_

       Registers a job(specification).
       A new job spec. comes into the prodagent, and
       if will be registerd.

       input:

       -jobSpecId (internal to the ProdAgent).
       -jobType (e.g. ProcessJob, MergeJob,....)
       -maxRetries (the number of times we can re submit the job
       if something goes wrong.
       -maxRacers. The maximum number of (the same) jobs
       we can submit simulatenously. For example if we have 95% of
       the request completed, we might want to submit 10 of the same
       jobs for the remaining 5%. Which ever job finishes first gives
       us the result (remaining jobs would be aborted). Setting the 
       maximum number of racers to 1 means effictively turning this
       feature off.
       -workflowID the id of the associated workflow

       output: nothing, or an exception
       """
       #WRAPPER
       #jobDetails={'id':jobSpecId,'job_type':jobType,'max_retries':maxRetries,\
       #   'max_racers':maxRacers}
       #Job.register(workflowID,None,jobDetails)

       sqlStr="""INSERT INTO js_JobSpec(JobSpecID,JobType,MaxRetries, MaxRacers,
       Retries,State,WorkflowID) VALUES("%s","%s","%s","%s","0","register","%s")
       """ %(str(jobSpecId),str(jobType),str(maxRetries),str(maxRacers),str(workflowID))
       try:
           Session.execute(sqlStr)
       except:
           raise ProdException(exceptions[3001]+":"+str(jobSpecId),3001)

def create(jobSpecId,cacheDir):
       """

       _create_

       Create job specification.
       Once a job spec. is registerd the necessary files and scripts
       can be created after which the state changes from "registered"
       to "create"

       -jobSpecId (internal to the ProdAgent).
       -cacheDir, location of the cacheDir. This dir contains the 
       job specification and the tarfile that will be used for grid
       submission.

       returns nothing or an error if the state change is not valid 
       (TransitionException).

       """
       #WRAPPER
       #Job.setState(jobSpecId,'create')
       #Job.setCacheDir(jobSpecId,cacheDir)

       sqlStr="UPDATE js_JobSpec SET State=\"create\",\
       CacheDirLocation=\""+cacheDir+"\" \
       WHERE JobSpecID=\""+jobSpecId+"\" AND State=\"register\";"
       rowsModified=Session.execute(sqlStr)
       if (rowsModified!=1):
           try:
              state=general(jobSpecId)['State']
           except:
              state="Undefined"
           raise ProdException(exceptions[3007]+str(state),3007)

def createFailure(jobSpecId):
       """
 
       _createFailure_
 
       Called when creation of a job fails. 

       -jobSpecId (internal to the ProdAgent).

       returns nothing or an error if the state change is not valid or
       a submit exception if the maximum numbers of tries is reached.

       """
       #WRAPPER
       #Job.registerFailure(jobSpecId,'create')

       sqlStr="UPDATE js_JobSpec SET Retries=Retries+1 WHERE "+ \
              " JobSpecID=\""+ jobSpecId +"\" AND "+ \
              " Retries<MaxRetries AND State=\"register\";"
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
           try:
               generalState=general(jobSpecId)
               state=generalState['State']
           except:
               state="Undefined"
           if  not state in ['create']:
               raise ProdException(exceptions[3012]+str(state),3012)
           # now check if we need to raise a sumit exception as
           # we have reached the maximum number of retries:
           #check if we have not reach the maximum number of retries
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1) ):
               raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)
           raise ProdException(exceptions[3014],3014)
       # now check if we need to raise a retry exception as
       # we have reached the maximum number of retries:
       generalState=general(jobSpecId)
       #check if we have not reach the maximum number of retries
       if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
           raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)


def inProgress(jobSpecId):
       """

       _inProgress_

       Job running in progress.
       Once a job spec. is created, jobs will be submitted which triggers
       the in progress state.

       submit, submitFailure running and runFailure are "micro" state
       changes of jobs embedded in the inProgress state. We do not record this,
       but assume that thes micro state changes are handled by job submission
       and tracking.  During submit we only check if it has not reached 
       the maximum number of submissions and maximum number of racers.

       -jobSpecId (internal to the ProdAgent).

       returns nothing or an error if the state change is not valid 
       (TransitionException)

       """
       #WRAPPER
       #Job.setState(jobSpecId,'inProgress')

       sqlStr="UPDATE js_JobSpec SET State=\"inProgress\"\
               WHERE JobSpecID=\""+jobSpecId+"\" AND State=\"create\";"
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
           try:
               state=general(jobSpecId)['State']
           except:
               state="Undefined"
           raise ProdException(exceptions[3008]+str(state),3008)

def submit(jobSpecId):
       """

       _submit_

       Job has been submitted.

       After a job(instance) has been created it will be submitted.
       This method is used for internal accounting of the number
       of submissions.

       -jobSpecId (internal to the ProdAgent).

       Returns an error if the state change is not valid (TransitionException)
       or if we have reached the maximum number of racer (RacerException), 
       or reach the maximum number of submission (SubmitException).
       """
       #WRAPPER
       #Job.setState(jobSpecId,'submit')

       sqlStr="UPDATE js_JobSpec SET "+    \
       "Racers=Racers+1 WHERE JobSpecID=\""+str(jobSpecId)+ \
       "\" AND (Racers+Retries)<MaxRetries AND "+ \
       "Racers<MaxRacers AND State=\"inProgress\";"
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
          try:
             generalState=general(jobSpecId)
             state=generalState['State']
          except:
             state="Undefined"
          if  not state in ['inProgress']:
             raise ProdException(exceptions[3018]+str(state),3018)
          #check if we have not reach the maximum number of retries
          if( (int(generalState['Retries'])+int(generalState['Racers']))> (int(generalState['MaxRetries'])-1)):
             raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)
           #check if we have not reach the maximum number of simulatneous 
           #jobs 
          if(int(generalState['Racers'])> (int(generalState['MaxRacers'])-1)):
              raise ProdException(exceptions[3015]+str(jobSpecId),3015)
          raise ProdException(exceptions[3014],3014)

def submitFailure(jobSpecId):
       """

       _submitFailure_

       Job submission failure.

       The submit states represent that the job is being
       submitted. If this goes wrong, the job will enter the
       submitFailure state.

       -jobSpecId (internal to the ProdAgent).

       Returns nothing or an error if the state change is not valid 
       (TransitionException) or if we have reached the minimum number of 
       racer (=0) (RacerException), or reach the maximum number of 
       submission (SubmitException).
       """
       #WRAPPER
       #Job.registerFailure(jobSpecId,'submit')

       sqlStr="UPDATE js_JobSpec SET Retries=Retries+1 "+\
       " WHERE JobSpecID=\""+ \
       jobSpecId+"\" AND Retries<MaxRetries AND State=\"inProgress\";"
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
           try:
               generalState=general(jobSpecId)
               state=generalState['State']
           except:
               state="Undefined"
           if  not state in ['inProgress']:
               raise ProdException(exceptions[3018]+str(state),3018)
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
               raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)
           raise ProdException(exceptions[3014],3014)
       # now check if we need to raise a retry exception as
       # we have reached the maximum number of retries:
       generalState=general(jobSpecId)
       #check if we have not reach the maximum number of retries
       if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
           raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)

# we assume we can extract the jobInstanceId 
# and runLocation from the job report. 
def runFailure(jobSpecId, jobInstanceId = None, \
               runLocation = None ,jobReportLocation = None):
       """

       _runFailure_

       Failure during running of the job.

       During the running of the job things can go wrong, which
       will be monitored by a jobTracker componenent. This will
       result in a state change to "runFailure". If we have reached
       the maximum number of submissions this method will generate
       a submit exception.

       -jobSpecId (internal to the ProdAgent).
       -jobInstanceID (we can retrieve this from the jobReport.
       -location where the job ran (we can retrieve this from the jobReport.
       -location of the job report (optional argument)

       Returns nothing or an error if the state change is not valid 
       (TransitionException) or if we have reached the minimum number of 
       racer (=0) (RacerException), or reach the maximum number of 
       submission (SubmitException).
       """
       #WRAPPER
       #Job.registerFailure(jobSpecId,'run')

       sqlStr="UPDATE js_JobSpec SET Retries=Retries+1,Racers=Racers-1 "+\
              "WHERE JobSpecID=\""+ jobSpecId+"\" AND "+\
              "Racers>0 AND Retries<MaxRetries AND State=\"inProgress\";"
       rowsModified=Session.execute(sqlStr)
       if rowsModified!=1:
           try:
               generalState=general(jobSpecId)
               state=generalState['State']
           except:
               state="Undefined"
           if(state!='inProgress'):
               raise ProdException(exceptions[3018]+str(state),3018)
           racers=int(generalState['Racers'])
           if(racers == 0):
               raise ProdException(exceptions[3016]+str(state),3016)
           if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
               raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)
           raise ProdException(exceptions[3017]+str(generalState['MaxRetries']),3017)
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
       Session.execute(sqlStr2)
       # now check if we need to raise a sumit exception as
       # we have reached the maximum number of retries:
       generalState=general(jobSpecId)
       #check if we have not reach the maximum number of retries
       if(int(generalState['Retries'])>(int(generalState['MaxRetries'])-1)):
           raise ProdException(exceptions[3013]+str(generalState['MaxRetries']),3013)

def finished(jobSpecId):
       """

       _finished_

       Job(specication) has finished.

       Once the running of the job completes successfully the proper
       component (jobTracker?) will change the state of the job to "finished"

       -jobSpecId (internal to the ProdAgent).

       returns nothing or an error if the state change is not valid 
       (TransisitionException)
       """
       #WRAPPER
       #Job.setState(jobSpecId,'finished')

       sqlStr1="""UPDATE js_JobSpec SET State="finished" WHERE 
                 JobSpecID="%s" AND State="inProgress";""" %(jobSpecId)
       rowsModified=Session.execute(sqlStr1)
       if rowsModified!=1:
           try:
              state=general(jobSpecId)['State']
           except:
              state="Undefined"
           raise ProdException(exceptions[3018]+str(state),3018)

def cleanout(jobSpecId):
      """
      _cleanout_

      Remove database entries associated to this jobSpecID.

      If the maximum number of submissions is reached,
      or the job has finished successfully, we can clean
      all the information about the job state.

      -jobSpecId (internal to the ProdAgent).

      returns nothing or an error if the state change is not valid
      (TransitionException)
      """
      #WRAPPER
      #Job.remove(jobSpecId)

      sqlStr4="""DELETE FROM js_JobSpec WHERE 
                JobSpecID="%s";""" %(jobSpecId)
      # not every mysql version supports cascade and foreign keys
      sqlStr3="""DELETE FROM js_JobInstance WHERE 
                 JobSpecID="%s";""" %(jobSpecId)
      sqlStr2="""DELETE FROM tr_Trigger WHERE 
                 JobSpecID="%s";""" %(jobSpecId)
      sqlStr1="""DELETE FROM tr_Action WHERE 
                 JobSpecID="%s";""" %(jobSpecId)
      Session.execute(sqlStr1)
      Session.execute(sqlStr2)
      Session.execute(sqlStr3)
      Session.execute(sqlStr4)

def setRacer(jobSpecId, maxRacers):
      """

      _setRacer_

      Set the number of racer.

      Sets the maximum number of the same jobs that can be run at the
      same time (number of racers). maxRacers represents the maximum 
      number of (the same) jobs we can submit simulatenously. For 
      example if we have 95% of the request completed, we might want 
      to submit 10 of the same jobs for the remaining 5%. Whichever 
      job finishes first gives us the result (remaining jobs would 
      be aborted). Setting the maximum number of racers to 1 means 
      effictively turning this feature off.
      """
      #WRAPPER
      #Job.setMaxRacers(jobSpecId,maxRacers)

      if maxRacers == 'max':
           sqlStr1="""
           UPDATE js_JobSpec SET Racers=MaxRacers+1,
           Retries=MaxRetries+1 WHERE
           JobSpecID="%s"; """ %(str(jobSpecId))
      else:
           sqlStr1="""UPDATE js_JobSpec SET MaxRacers="%s" WHERE
           JobSpecID="%s"; """ %(str(maxRacers),str(jobSpecId))
      rowsModified=Session.execute(sqlStr1)
      if rowsModified!=1:
         raise ProdException(exceptions[3019]+str(jobSpecId),3019)

def purgeStates():
      """
      _purgeStates_
 
      purges all state information (including trigger information) 

      """
      #WRAPPER
      #Job.removeAll()

      sqlStr1="""DELETE FROM js_JobSpec;"""
      sqlStr2="""DELETE FROM js_JobInstance;"""
      sqlStr3="""DELETE FROM tr_Trigger;"""
      sqlStr4="""DELETE FROM tr_Action;"""
      Session.execute(sqlStr1)
      # if cascacding is suported the next
      # queries are not needed.
      Session.execute(sqlStr2)
      Session.execute(sqlStr3)
      Session.execute(sqlStr4)

def setMaxRetries(jobSpecIds=[],maxRetries=1):
      """
      _setMaxRetries_

      Sets the number of max retries.

      """
      #WRAPPER
      #Job.setMaxRetries(jobSpecIds,maxRetries)

      if maxRetries<1:
         raise Exception("MaxRetries value should be larger than 0") 
      if type(jobSpecIds)==list:
         if len(jobSpecIds)==1:
            jobSpecIds=jobSpecIds[0]
         if len(jobSpecIds)==0:
            return
      Session.execute("START TRANSACTION")
      if type(jobSpecIds)==list:
         sqlStr=""" UPDATE js_JobSpec SET MaxRetries="%s" WHERE JobSpecID IN %s
         """ %(str(maxRetries),str(tuple(jobSpecIds)))
      else:
         sqlStr=""" UPDATE js_JobSpec SET MaxRetries="%s" WHERE JobSpecID="%s"
         """ %(str(maxRetries),str(jobSpecIds))
      Session.execute(sqlStr)


def general(JobSpecId):
       """
       _general_

       General information about a job specification.
       Given the JobSpecId this method returns
       a dictionary of the general information of the job.
       We use such a method to prevent multiple (costly)
       "small" queries to the database.

       input:
       -JobSpecId Internal Id used by the prod agent.

       returns:
           {'JobType':..,
           'MaxRetries':..,
           'Retries':..,
           'State':..,
           'CacheDirLocation':..,
           'MaxRacers':..,
           'Racers':..,
           }

       or an error if the JobSpecId does not exists.
       """
       #WRAPPER
       #jobDetails=Job.get(JobSpecId)
       #result={}
       #result['JobType']=jobDetails['job_type']
       #result['MaxRetries']=int(jobDetails['max_retries'])
       #result['Retries']=int(jobDetails['retries'])
       #if(jobDetails['status']=='in_progress'):
       #    result['State']='inProgress'
       #else:
       #    result['State']=jobDetails['status']
       #result['CacheDirLocation']=jobDetails['cache_dir']
       #result['MaxRacers']=int(jobDetails['max_racers'])
       #result['Racers']=int(jobDetails['racers'])
       #return result
       
       sqlStr='SELECT JobType,MaxRetries,Retries, '+\
              'State,CacheDirLocation, MaxRacers, Racers '+ \
              'FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       Session.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=Session.fetchall()
       if len(rows)==0:
           raise ProdException(exceptions[3019]+str(JobSpecId),3019)
       # format it in a dictionary
       return {'JobType':rows[0][0], \
               'MaxRetries':rows[0][1], \
               'Retries':rows[0][2], \
               'State':rows[0][3], \
               'CacheDirLocation':rows[0][4], \
               'MaxRacers':rows[0][5], \
               'Racers':rows[0][6] \
              }

def isRegistered(JobSpecId):
       #WRAPPER
       #return Job.exists(JobSpecId)   

       sqlStr='SELECT JobType FROM js_JobSpec WHERE '+  \
              'JobSpecID="'+JobSpecId+'";'
       Session.execute(sqlStr)
       #due to the schema we either get 0 or 1 row back.
       rows=Session.fetchall()
       if len(rows)==0:
           return False
       return True

def lastLocations(JobSpecId):
       """

       _lastLocations_

       Last locations of where jobs have been submitted.
       Returns the last locations where this job
       has been submitted, or an error if no
       location was found.

       input:
       -JobSpecId Internal Id used by the prod agent.

       returns:
       -an array with locations or an error
       """
       sqlStr='SELECT Location from js_JobInstance WHERE JobSpecID="'+\
       JobSpecId+'";'
       Session.execute(sqlStr)
       rows=Session.fetchall()
       if len(rows)==0:
           raise ProdException(exceptions[3020]+str(JobSpecId),3020)
       result=[]
       for i in rows:
           result.append(i[0])
       return result

def jobReports(JobSpecId):
       """

       _jobReports_

       Returns the locations of the job reports of failed jobs.
       Returns an array of job report locations associated to a job that
       has failed multiple times.

       input:
       -JobSpecId Internal Id used by the prod agent.

       returns:
       -an array of strings representing job report locations (xml files)
       or an array
       """
       sqlStr='SELECT JobReportLocation FROM js_JobInstance WHERE '+ \
       'JobSpecID="'+JobSpecId+'" AND JobReportLocation<>"NULL";'
       Session.execute(sqlStr)
       #this query will not return many (= thousands) of results.
       rows=Session.fetchall()
       result=[]
       #convert to an array:
       #NOTE: can this be done more efficient?
       for i in rows:
          result.append(i[0])
       return result

def jobSpecTotal():
       #WRAPPER
       #return Job.amount()
      
       sqlStr='SELECT COUNT(JobSpecID) FROM js_JobSpec;'
       Session.execute(sqlStr)
       #this query will not return many (= thousands) of results.
       rows=Session.fetchall()
       result=rows[0][0]
       return result

def rangeGeneral(start = -1, nr = -1):
       #WRAPPER
       #return Job.getRange(start,nr)
       
       if ( (start == -1) and (nr == -1) ):
           start=0
           nr=jobSpecTotal()
       if start<0:
           raise ProdException('Start should be larger than 0!')
       elif nr<0:
           raise ProdException('Number should be larger than 0!')
       sqlStr='SELECT JobSpecID, JobType,MaxRetries,Retries, '+\
              'State,CacheDirLocation, MaxRacers, Racers '+ \
              'FROM js_JobSpec LIMIT '+str(start)+','+str(nr)+';'
       Session.execute(sqlStr)
       #NOTE: we kind off assume that we only deal with small subsets.
       rows=Session.fetchall()
       result=[]
       resultDescription=['JobSpecID','JobType','MaxRetries','Retries','State','CacheDirLocation','MaxRacers','Racers']
       result.append(resultDescription)
       for row in rows:
           resultRow=[]
           for entry in row:
               resultRow.append(entry)
           result.append(resultRow)
       return result

def retrieveJobIDs(workflowIDs=[]):
    if type(workflowIDs)==list:
       if len(workflowIDs)==1:
          workflowIDs=workflowIDs[0]
       if len(workflowIDs)==0:
           return
    if type(workflowIDs)==list:
       sqlStr=""" SELECT JobSpecID FROM js_JobSpec WHERE WorkflowID IN %s
           """ %(str(tuple(workflowIDs)))
    else:
       sqlStr=""" SELECT JobSpecID FROM js_JobSpec WHERE WorkflowID="%s"
           """ %(str(workflowIDs))
    Session.execute(sqlStr)
    result=Session.fetchall()
    return result
