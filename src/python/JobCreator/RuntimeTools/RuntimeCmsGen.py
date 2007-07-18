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


class CmsGenSetup:

    def __init__(self, jobSpecFile):
        jobSpec = JobSpec()
        jobSpec.load(jobSpecFile)
        
        taskState = TaskState(os.getcwd())
        taskState.loadRunResDB()
        config = taskState.configurationDict()
        finder = NodeFinder(taskState.taskName())
        jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        self.checkArgs()
        self.writeCfg()

        self.createArgs()

    def checkArgs(self):
        """
        _checkArgs_

        Check required Args are present

        """
        keysList = [
            'generator', 'firstRun', 'maxEvents',
            'randomSeed', 'fileName',

            ]

        for key in keysList:
            if self.jobSpecNode.applicationControls.get(key, None) == None:
                msg = "CmsGen Argument %s not provided" % key
                msg += "Cannot generate cmsGen configuration"
                handle = open("exit.status", "w")
                handle.write("10040")
                handle.close()
                print msg
                raise RuntimeError, msg
                

    def writeCfg(self):
        """
        _writeCfg_

        Write the generator cfg file out

        """
        handle = open("CmsGen.cfg", "w")
        handle.write(self.jobSpecNode.configuration)
        handle.close()
        return

    def createArgs(self):
        """
        _createArgs_

        Create the command line args file

        """
        args = ""
        args += " --generator=%s " % (
            self.jobSpecNode.applicationControls['generator'],)
        args += " --run=%s "  % (
            self.jobSpecNode.applicationControls['firstRun'],)

        args += " --seed=%s " % (
            self.jobSpecNode.applicationControls['randomSeed'],)
        
        args += " --number-of-events=%s " % (
            self.jobSpecNode.applicationControls['maxEvents'],)

        args += " --job-report=FrameworkJobReport.xml "
        args += " --cfg=CmsGen.cfg "
        args += " --output-file=%s " % (
            self.jobSpecNode.applicationControls['fileName'],)
    

        print "Generated CmsGen Args:"
        print args
        print "Written to file: cmsGen.args"
    
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
    
