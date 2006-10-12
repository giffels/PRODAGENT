#!/usr/bin/env python
"""
_overrideStageOut_

Util to add a StageOut override to a workflow or JobSpec

"""

import os
import sys
import getopt




from MCPayloads.WorkflowSpec import WorkflowSpec
from MCPayloads.JobSpec import JobSpec
from MCPayloads.PayloadNode import listAllNames
from MCPayloads.WorkflowTools import addStageOutOverride


valid = ['workflow-spec=', 'job-spec=', 'stage-out-node=',
         'command=', 'option=', 'se-name=', 'lfn-prefix=']


usage = \
"""
Usage:  overrideStageOut.py --workflow-spec=<WorkflowSpec>
        -OR-
        overrideStageOut.py --job-spec=<JobSpec>

You must provide one of --workflow-spec or --job-spec.

Required Options

--stage-out-node=<Name>  The name of the stage out node in the spec that
   you want to override

--command=<Value>  The stage out command name
--option=<Value>   Any options for the command. Defaults to ""
--se-name=<Value>  The name of the SE that the stage out goes to
--lfn-prefix=<Value> The prefix to add to the LFN to make the PFN

The Values are the same as for a fallback stage out specified in a site conf.

"""


workflowSpec = None
jobSpec = None
stageOutNode = None
override = {
    "command" : None,
    "option" : "",
    "se-name" : None,
    "lfn-prefix" : None,
    }

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

for opt, arg in opts:
    if opt == "--workflow-spec":
        workflowSpec = arg
    if opt == "--job-spec":
        jobSpec = arg
    if opt == "--stage-out-node":
        stageOutNode = arg
    if opt == "--command":
        override['command'] = arg
    if opt == "--option":
        override['option'] = arg
    if opt == "--se-name":
        override['se-name'] = arg
    if opt == "--lfn-prefix":
        override['lfn-prefix'] = arg



if (workflowSpec == None) and (jobSpec == None):
    msg = "Error: You must provide one of --workflow-spec OR --job-spec"
    raise RuntimeError, msg

if (workflowSpec != None) and (jobSpec != None):
    msg = "Error: Both --workflow-spec and --job-spec are provided\n"
    msg += "You must provide only one of these"
    raise RuntimeError, msg


if stageOutNode == None:
    msg = "Error: --stage-out-node is not provided. This is required"
    raise RuntimeError, msg


for key, val in override.items():
    if val == None:
        msg = "--%s Option Not Provided\n" % key
        msg += "This option is required\n"
        raise RuntimeError, msg

    
if workflowSpec != None:
    if not os.path.exists(workflowSpec):
        msg = "Workflow Spec file Not Found:\n%s\n" % workflowSpec
        raise RuntimeError, msg
    spec = WorkflowSpec()
    spec.load(workflowSpec)
    specFile = workflowSpec

if jobSpec != None:
    if not os.path.exists(jobSpec):
        msg = "Job Spec file Not Found:\n%s\n" % jobSpec
        raise RuntimeError, msg

    spec = JobSpec()
    spec.load(jobSpec)
    specFile = jobSpec

allNames = listAllNames(spec.payload)
if stageOutNode not in allNames:
    msg = "Error: Cannot find Node named %s in spec\n" % stageOutNode
    msg += "Node names are: %s" % allNames
    raise RuntimeError, msg

class NodeFinder:
    def __init__(self, name):
        self.name = name
        self.result = None
    def __call__(self, node):
        if node.name == self.name:
            self.result = node
        return

finder = NodeFinder(stageOutNode)
spec.payload.operate(finder)
node = finder.result


if not node.type == "StageOut":
    msg = "Node %s is not a StageOut node\n" % stageOutNode
    msg += "It is a node of type: %s\n" % node.type
    raise RuntimeError, msg

node.configuration = ""

addStageOutOverride(node, override['command'],
                    override['option'],
                    override['se-name'],
                    override['lfn-prefix'])


spec.save(specFile)
sys.exit(0)





