#!/usr/bin/env python
"""
_TestHarness_

Testing setup for trying out and debugging the JobSubmitters.

Instantiates a Submitter with a command line provided 
job name and job area

Usage Example:

python TestHarness.py --jobname=Test1 --dir=/path/to/job/Test1 --submitter=<name of submitter to use>

--dir provides the location where the job will be created
--jobname name of job
--submitter name of submitter to use

"""
import os
import sys
import getopt
import logging
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

import JobSubmitter
from JobSubmitter.Registry import retrieveSubmitter
from ProdCommon.MCPayloads.JobSpec import JobSpec

if __name__ == '__main__':
    usage = "Usage: TestHarness.py --dir=<job dir>\n"
    usage += "                      --jobname=<name of job>\n"
    usage += "                      --submitter=<submitter name>\n"
    valid = ['dir=', 'jobname=', "submitter=", "job-spec-file="]
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", valid)
    except getopt.GetoptError, ex:
        print usage
        print str(ex)
        sys.exit(1)

    workingDir = None
    jobname = None
    submitter = "NoSubmit"
    jobSpecFile = None
    for opt, arg in opts:
        if opt == "--dir":
            workingDir = arg
        if opt == "--jobname":
           jobname = arg
        if opt == "--submitter":
            submitter = arg
        if opt == "--job-spec-file":
            jobSpecFile = arg
    if workingDir == None:
        print "No Working dir specified: --dir option is required"
        sys.exit(1)
    if not os.path.exists(workingDir):
        print "Working Dir Does Not Exist:"
        print workingDir
        sys.exit(1)

    if jobname == None:
        print "No Jobname specified: --jobname option is required"
        sys.exit(1)

    
    
    #  //
    # // load the submitter
    #//
    logging.info("TestHarness:Instantiating Submitter %s" % submitter)
    submitterInstance = retrieveSubmitter(submitter)
    logging.info("TestHarness:Submitter Instantiated OK")
    
    
    jobToSubmit = os.path.join(workingDir, jobname)
    if jobSpecFile == None:
        jobSpecFile = os.path.join(workingDir, "%s-JobSpec.xml" % jobname)
    cacheMap = { jobname : workingDir }
    logging.debug("TestHarness:Jobname=%s" % jobname)
    logging.debug("TestHarness:WorkingDir=%s" % workingDir)
    logging.debug("TestHarness:JobToSubmit=%s" % jobToSubmit)
    logging.debug("TestHarness:JobSpecFile=%s" % jobSpecFile)
    
    try:
        jobSpecInstance = JobSpec()
        jobSpecInstance.load("file://%s" % jobSpecFile)
    except StandardError, ex:
        msg = "TestHarness:Failed to read JobSpec File for Job %s\n" % jobname
        msg += "From: %s\n" % jobSpecFile
        msg += str(ex)
        logging.error(msg)
        sys.exit(1)

    
    
           
    
    logging.info("TestHarness: Invoking Submitter %s" % submitter)
    submitterInstance(
        workingDir,
        jobToSubmit, jobname,
        JobSpecInstance = jobSpecInstance,
        CacheMap = cacheMap
        )

    
