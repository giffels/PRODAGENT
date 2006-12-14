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

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from JobCreator.Registry import retrieveGenerator, retrieveCreator
import JobCreator.Creators
import JobCreator.Generators


usage = "Usage: TestHarness.py --dir=<job creation dir>\n"
usage += "                      --job-spec=<jobspec XML>\n"
usage += "                      --workflow-spec=<workflow spec XML>\n"
usage += "                      --creator=<creator name>\n"
usage += "                      --generator=<generator name>\n"
valid = ['dir=', 'workflow-spec=', 'job-spec=', "creator=", "generator="]
try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workingDir = None
workflowSpecFile = None
jobSpecFile = None
creator = None
generator = None

for opt, arg in opts:
    if opt == "--dir":
        workingDir = arg
    if opt == "--job-spec":
        jobSpecFile = arg
    if opt == "--workflow-spec":
        workflowSpecFile = arg
    if opt == "--creator":
        creator = arg
    if opt == "--generator":
        generator = arg
    
if workingDir == None:
    print "No Working dir specified: --dir option is required"
    sys.exit(1)
if not os.path.exists(workingDir):
    print "Working Dir Does Not Exist:"
    print workingDir
    sys.exit(1)

if jobSpecFile == None:
    print "No JobSpec file specified: --job-spec option is required"
    sys.exit(1)
if not os.path.exists(jobSpecFile):
    print "JobSpec File does not exist:"
    print jobSpecFile
    sys.exit(1)

if workflowSpecFile == None:
    print "No Workflow Spec file specified: --workflow-spec option is required"
    sys.exit(1)
if not os.path.exists(workflowSpecFile):
    print "Workflow Spec File does not exist:"
    print workflowSpecFile
    sys.exit(1)


try:
    jobSpec = JobSpec()
    jobSpec.load(jobSpecFile)
except StandardError, ex:
    msg = "Error loading job spec file:\n"
    msg += jobSpecFile
    msg += "\n"
    msg += str(ex)
    print msg
    sys.exit(1)

try:
    workflowSpec = WorkflowSpec()
    workflowSpec.load(workflowSpecFile)
except Exception, ex:
    msg = "Error loading workflow spec file:\n"
    msg += workflowSpecFile
    msg += "\n"
    msg += str(ex)
    print msg
    sys.exit(1)

if creator == None:
    print "No creator specified: --creator option is required"
    sys.exit(1)
if generator == None:
    print "No generator specified: --generator option is required"
    sys.exit(1)



#  //
# // Load the generator and process the workflow spec
#//
wfname = workflowSpec.workflowName()
wfCache = os.path.join(workingDir, wfname)
if not os.path.exists(wfCache):
    os.makedirs(wfCache)
    
    
gen = retrieveGenerator(generator)
creatorInst = retrieveCreator(creator)
gen.creator = creatorInst
gen.actOnWorkflowSpec(workflowSpec, wfCache)


del gen
del creatorInst
#  //
# // Now process the job spec 
#//
jobname = jobSpec.parameters['JobName']
jobCache = os.path.join(wfCache, jobname)
if not os.path.exists(jobCache):
    os.makedirs(jobCache)

        
gen = retrieveGenerator(generator)
creatorInst = retrieveCreator(creator)
gen.creator = creatorInst
gen.actOnJobSpec(jobSpec, jobCache)

