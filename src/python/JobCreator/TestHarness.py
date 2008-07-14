#!/usr/bin/env python
"""
_TestHarness_

Testing setup for trying out and debugging the JobCreators.

Instantiates a JobGenerator with a command line provided 
JobSpec file and working dir to create a test job

Usage Example:

python TestHarness.py --dir=/tmp/testjobs --spec=/path/to/JobSpec.xml --creator=testCreator

--dir provides the location where the job will be created
--spec provides a JobSpec XML file to create the job from
--creator provides the name of the creator to create the job

"""
import os
import sys
import getopt
import logging
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

from MCPayloads.JobSpec import JobSpec
from JobCreator.JobGenerator import JobGenerator
import JobCreator.Creators

if __name__ == '__main__':
    usage = "Usage: TestHarness.py --dir=<job creation dir>\n"
    usage += "                      --spec=<jobspec XML>\n"
    usage += "                      --creator=<creator name>\n"
    usage += "                      --submitter=<submitter name>\n"
    valid = ['dir=', 'spec=', "creator=", "submitter="]
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", valid)
    except getopt.GetoptError, ex:
        print usage
        print str(ex)
        sys.exit(1)

    workingDir = None
    specfile = None
    creator = None
    submitter = "noSubmit"
    for opt, arg in opts:
        if opt == "--dir":
            workingDir = arg
        if opt == "--spec":
            specfile = arg
        if opt == "--creator":
            creator = arg
        if opt == "--submitter":
            submitter = arg
        
    if workingDir == None:
        print "No Working dir specified: --dir option is required"
        sys.exit(1)
    if not os.path.exists(workingDir):
        print "Working Dir Does Not Exist:"
        print workingDir
        sys.exit(1)

    if specfile == None:
        print "No JobSpec file specified: --spec option is required"
        sys.exit(1)
    if not os.path.exists(specfile):
        print "JobSpec File does not exist:"
        print specfile
        sys.exit(1)
    try:
        jobSpecInstance = JobSpec()
        jobSpecInstance.load(specfile)
    except StandardError, ex:
        msg = "Error loading job spec file:\n"
        msg += specfile
        msg += "\n"
        msg += str(ex)
        print msg
        sys.exit(1)

    if creator == None:
        print "No creator specified: --creator option is required"
        sys.exit(1)

    #  //
    # // Initialise the JobGenerator 
    #//
    jobGen = JobGenerator(jobSpecInstance, {"ComponentDir" : workingDir,
                                            "CreatorName"  : creator,
                                            "SubmitterName" : submitter})
    
    #  //
    # // Invoke the creator
    #//
    jobGen()
