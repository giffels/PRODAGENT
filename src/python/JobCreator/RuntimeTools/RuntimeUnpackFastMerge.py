#!/usr/bin/env python
"""
_RuntimeUnpackFastMerge_


Generate list of input files for FastMerge job at runtime
from the contents of the JobSpec file

"""


import sys
import os
from FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.JobSpec import JobSpec

class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance


class FastMergeUnpacker:
    """
    _FastMergeUnpacker_

    Extract the list of input files from the JobSpec PSet

    """
    def __init__(self, jobSpecFile):
        self.jobSpec = JobSpec()
        self.jobSpec.load(jobSpecFile)
        self.taskState = TaskState(os.getcwd())
        self.taskState.loadRunResDB()
        self.config = self.taskState.configurationDict()

        finder = NodeFinder(self.taskState.taskName())
        self.jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        self.jobSpecNode.loadConfiguration()
        
        cfgInt = self.jobSpecNode.cfgInterface
        inputFiles = cfgInt.inputSource.fileNames()
        fileList = ""
        for inputfile in inputFiles:
            inputfile = inputfile.replace("\'", "")
            inputfile = inputfile.replace("\"", "")
            fileList += "%s " % inputfile

        handle = open("EdmFastMerge.input", "w")
        handle.write(fileList)
        handle.close()
        
            

if __name__ == '__main__':
    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg += "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = FastMergeUnpacker(jobSpec)
    
