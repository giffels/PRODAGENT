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
       jobDetails={'id':jobSpecId,'job_type':jobType,'max_retries':maxRetries,\
          'max_racers':maxRacers}
       Job.register(workflowID,None,jobDetails)
       
  

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
       Job.setState(jobSpecId,'create')
       Job.setCacheDir(jobSpecId,cacheDir)

def createFailure(jobSpecId):
       """
 
       _createFailure_
 
       Called when creation of a job fails. 

       -jobSpecId (internal to the ProdAgent).

       returns nothing or an error if the state change is not valid or
       a submit exception if the maximum numbers of tries is reached.

       """
       #WRAPPER
       Job.registerFailure(jobSpecId,'create')

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
       Job.setState(jobSpecId,'inProgress')

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
       Job.setState(jobSpecId,'submit')

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
       Job.registerFailure(jobSpecId,'submit')


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
       Job.registerFailure(jobSpecId,'run')

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
       Job.setState(jobSpecId,'finished')

def failed(jobSpecId):
       """

       _failed_

       Job(specication) has failed.

       returns nothing or an error if the state change is not valid 
       """
       #WRAPPER
       Job.setState(jobSpecId,'failed')


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
      Job.remove(jobSpecId)


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
      Job.setMaxRacers(jobSpecId,maxRacers)

def purgeStates():
      """
      _purgeStates_
 
      purges all state information (including trigger information) 

      """
      #WRAPPER
      Job.removeAll()

def setMaxRetries(jobSpecIds=[],maxRetries=1):
      """
      _setMaxRetries_

      Sets the number of max retries.

      """
      #WRAPPER
      Job.setMaxRetries(jobSpecIds,maxRetries)


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
       jobDetails=Job.get(JobSpecId)
       result={}
       result['JobType']=jobDetails['job_type']
       result['MaxRetries']=int(jobDetails['max_retries'])
       result['Retries']=int(jobDetails['retries'])
       if(jobDetails['status']=='in_progress'):
           result['State']='inProgress'
       else:
           result['State']=jobDetails['status']
       result['CacheDirLocation']=jobDetails['cache_dir']
       result['MaxRacers']=int(jobDetails['max_racers'])
       result['Racers']=int(jobDetails['racers'])
       return result
       
      
def isRegistered(JobSpecId):
       #WRAPPER
       return Job.exists(JobSpecId)   

def doNotAllowMoreSubmissions(jobSpecIds = []):
       """
       _doNotAllowSubmission_
 
       Set racers to maxRacers + 1 and retries to maxRetries + 1
 
       """
       for jobSpecId in jobSpecIds:
           sqlStr="UPDATE we_Job SET "+    \
                  "racers=max_racers+1, retries=max_retries+1 "+ \
                  "WHERE id=\""+ str(jobSpecId)+ "\";"
           Session.execute(sqlStr)

def jobSpecTotal():
       #WRAPPER
       return Job.amount()
      
def rangeGeneral(start = -1, nr = -1):
       #WRAPPER
       return Job.getRange(start,nr)
       
def retrieveJobIDs(workflowIDs=[]):
    if type(workflowIDs)==list:
       if len(workflowIDs)==1:
          workflowIDs=workflowIDs[0]
       if len(workflowIDs)==0:
           return
    if type(workflowIDs)==list:
       sqlStr=""" SELECT id FROM we_Job WHERE workflow_id IN %s
           """ %(str(tuple(workflowIDs)))
    else:
       sqlStr=""" SELECT id FROM we_Job WHERE workflow_id="%s"
           """ %(str(workflowIDs))
    Session.execute(sqlStr)
    result=Session.fetchall()
    return result
