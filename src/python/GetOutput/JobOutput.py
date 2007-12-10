#!/usr/bin/env python
"""
_Joboutput_

Deals with job get output operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__version__ = "$Id$"
__revision__ = "$Revision$"

import logging
from ProdAgentBOSS import BOSSCommands
from GetOutput.TrackingDB import TrackingDB
from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SafeSession import SafeSession

###############################################################################
# Class: JobOutput                                                            #
###############################################################################

class JobOutput:
    """
    A static instance of this class deals with job get output operations
    """

    # default parameters
    params = {'maxGetOutputAttempts' : 3,
              'bossCfgDir' : '.',
              'dbInstance' : None}

    def __init__(self):
        """
        Attention: in principle, no instance of this static class is created!
        """
        
        pass

    @classmethod
    def setParameters(cls, params):
        """
        set parameters
        """

        cls.params = params

    @classmethod
    def requestOutput(cls, jobInfo):
        """
        request output for job.

        """

        # get job data
        jobId = jobInfo['jobId']
        jobSpecId = jobInfo['jobSpecId']
        directory = jobInfo['directory']
        bossStatus = jobInfo['bossStatus']

        # open database
        session = SafeSession(dbInstance = cls.params['dbInstance'])
        db = TrackingDB(session)

        # get job info
        job = db.getJobInfo(jobId)

        # add the job if it does not exist
        if job == {}:
            db.addJobs([jobId])
            job = db.getJobInfo(jobId)

        # verify status
        if job['status'] != 'output_not_requested':
            logging.error("Job %s is in status %s, cannot request output" % \
                          (jobId, job['status']))
            session.close()
            return

        # set job info and modify status
        modified = db.setJobInfo(jobId,
                                 status = 'output_requested',
                                 jobSpecId = jobSpecId,
                                 directory = directory,
                                 bossStatus = bossStatus)

        # interrupted opearation, already enqueued
        if modified == 0:

            logging.error("Output for job %s cannot be requested" % jobId)
            session.close()
            return

        # commit and close session
        session.commit()
        session.close()

        logging.debug("getoutput request for %s successfully enqueued" % jobId)

    @classmethod
    def doWork(cls, jobId):
        """
        get the output of the job specified.

        *** thread safe ***

        """

        try:

            logging.debug("Getting output for job" + str(jobId))

            # open database
            session = SafeSession(dbInstance = cls.params['dbInstance'])
            db = TrackingDB(session)

            # get job info
            jobInfo = db.getJobInfo(jobId)

            # verify the job exists
            if jobInfo == {}:
                logging.error("Cannot get output, job %s is not registered" % \
                              jobId)
                session.close()
                return jobId

            # verify the status 
            status = jobInfo['status']

            # output retrieved before, then recover interrupted operation
            if status == 'output_retrieved':
                logging.warning("Enqueuing previous ouput for job %s" % \
                              jobId)
                session.close()
                return jobId

            # non expected status, abandon processing for job
            if status != 'in_progress':
                logging.error("Cannot get output for job %s, status is %s" % \
                              (jobId, jobInfo['status']))
                session.close()
                return jobId

            #  get output, trying at most maxGetOutputAttempts
            retry = 0
            while retry < cls.params['maxGetOutputAttempts']:

                # perform get output operation
                try:
                    output = BOSSCommands.getoutput(jobId, \
                                                    jobInfo['directory'], \
                                                    cls.params['bossCfgDir'])
                    break

                # error, update retry counter
                except Exception, msg:
                    logging.error(str(msg))
                    output = "error"
                    retry += 1

            # check for empty output
            if output == '':
                output = "error: no output retrived by BOSSGetoutput command"

            # update quotes in output
            output = output.replace("'", '"')

            # update info in database
            updateStatus = db.setJobInfo(jobId, status = 'output_retrieved', \
                                         output = output)
            if updateStatus != 1:
                logging.warning("Output not updated for job %s: %s" % \
                                (jobId,updateStatus))
            else:
                logging.debug("Output for job %s successfully enqueued" % \
                              jobId)

            # done, commit and finish
            session.commit()
            session.close()

            # return job info
            return jobId

        # thread has failed
        except Exception, msg:

            # show error message 
            msg = "GetOutputThread exception: %s" % str(msg)
            logging.error(msg)

            # return also the id
            return jobId
 
    @classmethod
    def recreateOutputOperations(cls, pool):
        """
        _recreateOutputOperations_

        recreate interrupted get output operations. Two types of operations
        can be in interrupted states: 1) operations that have been
        requested but never performed, and 2) operations that have been
        performed but never processed (meaning that output was retrieved
        but no messages were sent based on it).

        """

        logging.debug("Recreating interrupted operations")

        # open database
        session = SafeSession(dbInstance = cls.params['dbInstance'])
        db = TrackingDB(session)

        # get interrupted operations
        jobs = db.getJobs(status="in_progress") + \
               db.getJobs(status="output_retrieved")
        numberOfJobs = len(jobs)

        logging.debug("Going to recreate %s get output requests" % \
                      numberOfJobs)

        # enqueue requests
        for job in jobs:
            pool.enqueue(job, job)

        # close database
        session.close()
        logging.debug("Recreated %s get output requests" % numberOfJobs)

    @classmethod
    def setDoneStatus(cls, jobId):
        """
        _setDoneStatus_

        signal finished status for get output operation

        """

        logging.debug("set done status for: " + str(jobId))

        session = SafeSession(dbInstance = cls.params['dbInstance'])

        # update job status
        query = """update """ + cls.params['bossDB'] + """.jt_activejobs
                      set status='output_processed'
                    where job_id='""" + jobId + """'
                """
        session.execute(query)

        # close database
        session.commit()
        session.close()

        logging.debug("Output processing done for %s", jobId)


