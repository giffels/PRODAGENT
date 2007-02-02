#!/usr/bin/env python
from JobState.Database.Api import JobStateInfoAPIMySQL

#NOTE: eventually you might want to have a more dynamic 
#NOTE: way of loading different persistency backends
#NOTE: although multiple backends results in more maintance

def general(jobId, dbCur=None):
       """
       _general_

       General information about a job specification.
       Given the JobSpecId this method returns
       a dictionary of the general information of the job.
       We use such a method to prevent multiple (costly)
       "small" queries to the database.

       input:
       -JobSpecId Internal Id used by the prod agent.
       -dbCur (optional). If it is used by an object which
       has a cursor instance, it can re use it.

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

       return JobStateInfoAPIMySQL.general(jobId, dbCur)

def isRegistered(jobId,dbCur = None):
       return JobStateInfoAPIMySQL.isRegistered(jobId, dbCur)

def lastLocations(jobId, dbCur = None):
       """

       _lastLocations_

       Last locations of where jobs have been submitted.
       Returns the last locations where this job
       has been submitted, or an error if no
       location was found.

       input:
       -JobSpecId Internal Id used by the prod agent.
       -dbCur (optional). If it is used by an object which
       has a cursor instance, it can re use it.

       returns:
       -an array with locations or an error
       """

       return JobStateInfoAPIMySQL.lastLocations(jobId, dbCur)

def jobReports(jobId, dbCur = None):
       """

       _jobReports_

       Returns the locations of the job reports of failed jobs.
       Returns an array of job report locations associated to a job that
       has failed multiple times.

       input:
       -JobSpecId Internal Id used by the prod agent.
       -dbCur (optional). If it is used by an object which calls this method.

       returns:
       -an array of strings representing job report locations (xml files)
       or an array
       """

       return JobStateInfoAPIMySQL.jobReports(jobId)

def jobSpecTotal(dbCur = None):

       return JobStateInfoAPIMySQL.jobSpecTotal(dbCur)

def rangeGeneral(start = -1, nr = -1, dbCur = None):

       return JobStateInfoAPIMySQL.rangeGeneral(start,nr,dbCur)

def startedJobs(daysBack):
      """
      _startedJobs_

      Returns a list of job spec ids that have started 
      futher than the number of days specified.

      """

      return JobStateInfoAPIMySQL.startedJobs(daysBack)


def retrieveJobIDs(workflowIDs=[]):
      return JobStateInfoAPIMySQL.retrieveJobIDs(workflowIDs)
