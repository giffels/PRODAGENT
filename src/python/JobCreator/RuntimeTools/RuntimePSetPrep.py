#!/usr/bin/env python
"""
_RuntimePSetPrep_

Runtime script that reads in the python PSet file, and
writes out the {{{}}} format PSet file.

May also require some localisation of parameters, expansion
of env vars to be done here in support of chained jobs.

"""

import sys
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

def unquote(strg):
    """remove leading and trailing quotes from string"""
    while strg.startswith("\'") or strg.startswith("\""):
        strg = strg[1:]
    while strg.endswith("\'") or strg.endswith("\""):
        strg = strg[:-1]
    return strg   


class JobSpecExpander:

    def __init__(self, jobSpecFile):
        self.jobSpec = JobSpec()
        self.jobSpec.load(jobSpecFile)
        self.taskState = TaskState(os.getcwd())
        self.taskState.loadRunResDB()
        
        
        self.config = self.taskState.configurationDict()

        finder = NodeFinder(self.taskState.taskName())
        self.jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        
        if self.config.has_key('Configuration'):
            try:
                self.createPSet()
            except Exception, ex:
                msg = "Unable to generate cmsRun Config from JobSpec:\n"
                msg += str(ex)
                print msg
                badfile = open("exit.status", 'w')
                badfile.write("10040")
                badfile.close()

        
            


    def createPSet(self):
        """
        _createPSet_

        Create the PSet cfg File

        """
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.cfg")[0]
        cfgFile = str(cfgFile)
        self.jobSpecNode.loadConfiguration()
        cmsProcess = self.jobSpecNode.cfgInterface.makeConfiguration()

        handle = open(cfgFile, 'w')
        handle.write(cmsProcess.dumpConfig())
        handle.close()
        
        return
        


if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]

    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg += "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = JobSpecExpander(jobSpec)
    
    
    












