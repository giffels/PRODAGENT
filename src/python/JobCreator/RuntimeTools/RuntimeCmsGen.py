#!/usr/bin/env python
"""
_RuntimeCmsGen_

Runtime Tool for preparing input file args for cmsGen

Should read the JobSpec file and generate the cmsGen.args script containing
the command line arguments for a cmsGen job.

"""

import os
from ProdCommon.MCPayloads.JobSpec import JobSpec
from FwkJobRep.TaskState import TaskState


class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance


def cmsGenSetup(jobSpecFile):
    jobSpec = JobSpec()
    jobSpec.load(jobSpecFile)
    
    taskState = TaskState(os.getcwd())
    taskState.loadRunResDB()
    config = taskState.configurationDict()
    finder = NodeFinder(taskState.taskName())
    jobSpec.payload.operate(finder)
    jobSpecNode = finder.result
    
    args = ""
    args += " --executable=%s " % jobSpecNode.applicationControls['executable']
    args += " --generator=%s " % jobSpecNode.applicationControls['generator']
    
    
    
    
    handle = open("cmsGen.args", "w")
    handle.write(args)
    handle.close()
    


if __name__ == '__main__':
    msg = "*****Invoking RuntimeCmsGen Prep script*****"
    print msg

    
    
    jobSpecLocation = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpecLocation == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpecLocation):
        msg += "Cannot find JobSpec file:\n %s\n" % jobSpecLocation
        msg += "Unable to proceed\n"
        raise RuntimeError, msg
    
    setupTool = CmsGenSetup(jobSpecLocation)
    
