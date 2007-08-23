#!/usr/bin/env python

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

from JobQueue.JobQueueDB import JobQueueDB


import sys, getopt


valid = [
    'workflow=',      # filter by workflow
    'site=',          # filter by site index
    'type=',          # filter by type
    'released',       # show jobs that have been released 
    'detailed',         # print out all job IDs
    ]

workflow = None
site = None
byType = None
releasedFlag = False
detailedFlag = False


usage = """
jobQueue.py Dump State of Job Queue to stdout
            --detailed
            --released
            --workflow=<WorkflowID>  # filter by workflow
            --site=<SiteIndex>       # filter by site index match
            --type=<Merge|Processing># filter by job type
            

"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


for opt, arg in opts:
    if opt == "--detailed":
        detailedFlag  = True
    if opt == "--released":
        releasedFlag  = True
    if opt == "--workflow":
        workflow = arg
    if opt == "--site":
        site = arg
    if opt == '--type':
        byType = arg
    if opt == "--released":
        releasedFlag = True

if byType not in (None, "Merge", "Processing"):
    msg = "Unknown job type: %s\n" % byType
    msg += "Must be Merge or Processing if specified\n"
    raise RuntimeError, msg


Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()

jobQueue = JobQueueDB()
jobQueue.loadSiteMatchData()





if site != None:
    if releasedFlag:
        jobIds = jobQueue.retrieveReleasedJobsAtSites(1000000, byType,
                                                      workflow, site)

    else:
        jobIds = jobQueue.retrieveJobsAtSites(1000000, byType,
                                              workflow, site)
else:
    if releasedFlag:

        jobIds = jobQueue.retrieveReleasedJobs(1000000, byType, workflow)
    else:
        jobIds = jobQueue.retrieveJobs(1000000, byType, workflow)
    





result = "Total of %s Jobs Retrieved matching constraints:\n" % len(jobIds)
result += " Workflow: %s\n" % workflow
result += " Site:     %s\n" % site
result += " Type:     %s\n" % byType


if detailedFlag:
    details = jobQueue.retrieveJobDetails(*jobIds)
    for detail in details:
        result += "%s job: %s\n" % (detail['JobType'], detail['JobSpecId'])
        

print result






Session.commit_all()
Session.close_all()
