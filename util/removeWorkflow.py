#!/usr/bin/env python
"""
_removeWorkflow_

Util to remove a workflow from the PA internal DBs.

Cleans up:

- JobQueue
- WorkflowEntities & Triggers
- MergeSensor

"""

import sys, os

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from MergeSensor.MergeSensorDB import MergeSensorDB
from JobQueue.JobQueueDB import JobQueueDB
import ProdAgent.WorkflowEntities.Aux as WEAux
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow

workflow = sys.argv[1]
workflowSpec = WorkflowSpec()
workflowSpec.load(workflow)

#  //
# // Clean out job cache
#//
config = loadProdAgentConfiguration()
compCfg = config.getConfig("JobCreator")
creatorCache = os.path.expandvars(compCfg['ComponentDir'])

workflowCache = os.path.join(creatorCache, workflowSpec.workflowName())
if os.path.exists(workflowCache):
    os.system("/bin/rm -rf %s" % workflowCache)



Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()

#  //
# // clean out queue
#//
jobQ = JobQueueDB()
jobQ.removeWorkflow(workflowSpec.workflowName())


#  //
# // workflow entities
#//
jobs = WEWorkflow.getJobIDs(workflowSpec.workflowName())
WEAux.removeJob(jobs)
WEAux.removeWorkflow(workflowSpec.workflowName())

#  //
# // merge sensor
#//
mergeDB = MergeSensorDB()
mergeDatasets = workflowSpec.outputDatasets()

for d in mergeDatasets:
    try:
        mergeDB.removeDataset(d.name()) 
    except Exception, ex:
        print "Skipping %s: %s" % (d, ex)
mergeDB.commit()

Session.commit_all()
Session.close_all()









