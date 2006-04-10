---++ Set up considerations:
-Make sure you grant access to the users this api uses 
(see ../ProdAgentDB/config.py)
-The code depends on the python MySQLdb package 
(http://sourceforge.net/projects/mysql-python)
-Make sure that the MySQL server has an InnoDB engine as
this supports row locking.
-Create the schema and give access (see ..sql/ProdAgentDB/*.sql, and
../ProdAgentDB/config/*.sql files)

---++ General Information:

The Job State API exposes an interface for components to update the state of a
jobspecification. It is an internal accounting module for the prodAgent.
The Job State API does not deal with individual submitted jobs other than
keeping track of the statistics. It is assumed this tracking of submitted jobs
will be done by the job tracker and job submission modules. Below a summary of
the API.

State modifiers methods:

<verbatim>
   * JobStateChangeAPI.register(jobSpecId, jobType, maxRetries, maxRacers):
   * JobStateChangeAPI.create(jobSpecId,tarfile):
   * JobStateChangeAPI.inProgress(jobSpecId):
   * JobStateChangeAPI.submit(jobSpecId):
   * JobStateChangeAPI.submitFailure(jobSpecId):
   * JobStateChangeAPI.runFailure(jobSpecId, jobInstanceId = None, \
                runLocation = None ,jobReportLocation = None):
   * JobStateChangeAPI.finished(jobSpecId):
   * JobStateChangeAPI.cleanout(jobSpecId):
   * JobStateChangeAPI.setRacer(jobSpecID,maxRacers)
</verbatim>

State info methods:

<verbatim>
   * JobStateInfo.general(jobSpecId):
   * JobStateInfo.lastLocations(jobSpecId):
   * JobStateInfo.jobReports(jobSpecId):
</verbatim>

---++ Usage:
Several components will need to use the JobStateChangeAPI. Below several 
examples of how to use it. More examples can be found in the
MCPROTO/test/python/UnitTests/JobState_t.py and
MCPROTO/test/python/UnitTests/JobState_t2.py file which use the 
python test environment.


Depending on the component you would call one (or several) methods. For 
example an error handler would call either a submitFailure or runFailure 
method when it receives an error related to jobs. While the component that 
handles the incoming request would call the register method.

<verbatim>
# import this package
from JobState.JobStateAPI import JobStateChangeAPI

   try:
     JobStateChangeAPI.register("jobClassID2","processing",2,1)
     JobStateChangeAPI.create("jobClassID2","tarfile/location/2.tar.gz")
     JobStateChangeAPI.inProgress("jobClassID2")
     JobStateChangeAPI.submit("jobClassID2")
     JobStateChangeAPI.submitFailure("jobClassID2")
     JobStateChangeAPI.runFailure("jobClassID2","jobInstanceID2.1",
         "some.location2.1","job/Report/Location2.1.xml")
     JobStateChangeAPI.submit("jobClassID2")
     JobStateChangeAPI.finished("jobClassID2")
     JobStateChangeAPI.cleanout("jobClassID2")
     JobStateChangeAPI.setRacer("jobClassID2",10)
   # you can of course also use only a general exception.
   except RacerException, ex:
     # do something if needed
   except SubmitException, ex:
     # do something if needed
   except TransitionException, ex:
     # do something if needed
   except Exception, ex:
     # do something if needed

</verbatim>
