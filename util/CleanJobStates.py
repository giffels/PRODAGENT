#!/usr/bin/env python
"""
_CleanJobStates_

Util to cleanup job state information for a workflow.

Maintainence tool, not for casual users

Usage:

python2.4 CleanJobStates.py --workflow=<WorkflowName>


"""

import re, sys, getopt
from ProdAgentDB.Connect import connect
from JobState.JobStateAPI.JobStateChangeAPI import cleanout

valid = [ 'workflow=', 'test']

workflow = None
testmode = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--test":
        testmode = True

if workflow == None:
    msg = "Error: --workflow not provided"
    raise RuntimeError, msg


def listJobSpecIds():
    """
    _listJobSpecIds_

    List all job spec ids known in DB

    """
    
    sqlStr = "select JobSpecID from js_JobSpec;"

    connection = connect()
    dbCur = connection.cursor()

    dbCur.execute(sqlStr)
    rows  = dbCur.fetchall()
    dbCur.close()

    

    results = [ i[0] for i in rows ]
    return results



specs = listJobSpecIds()


class SpecFilter:
    def __init__(self, workflowName):
        self.matcher = re.compile("%s-[0-9]+" % workflowName)

    def __call__(self, jobspec):
        if self.matcher.match(jobspec):
            return True
        return False


specs = filter(SpecFilter(workflow), specs)

if testmode:
    print "TEST MODE: Nothing will be deleted..."

for spec in specs:
    print "Cleaning ID %s from JobStates" % spec
    if not testmode:
        cleanout(spec)






