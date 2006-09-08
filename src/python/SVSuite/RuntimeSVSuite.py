#!/usr/bin/env python
"""
_RuntimeSVSuite_

Binary file for running an SVSuite Task

"""
import sys
import os


from SVSuite.SVSuiteTask import SVSuiteTask
from SVSuite.Configuration import Configuration
from SVSuite.SVSuiteError import SVSuiteError

from FwkJobRep.TaskState import TaskState, getTaskState


def main():
    """
    _main_

    Main function to run an SVSuite Task

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()
    thisConfig = state.configurationDict()
    inputTask = thisConfig['SVSuiteParameters']['SVSuiteInput'][0]
    setupCommand = thisConfig['SVSuiteParameters']['SVSuiteSetupCommand'][0]

    inputState = getTaskState(inputTask)
    inputDir = inputState.dir

    
    
    configFile = "SVSuiteConfig.xml"
    if not os.path.exists(configFile):
        msg = "Config file  %s not found\n" % configFile
        msg += "Unable to run SVSuite\n"
        raise RuntimeError, msg
    
    svConfig = Configuration()
    svConfig.read(configFile)

    
    dataDir = os.path.join(os.getcwd(), "SVSuiteData")
    if not os.path.exists(dataDir):
        os.makedirs(dataDir)

        
    svConfig.svSuiteDataDir = dataDir
    svConfig.svSuiteInputDir = inputDir
    svConfig.svSuiteBinDir = os.path.join(dataDir, "SVSuite",
                                        "Validation", "Tools", "bin")
    svConfig.svSuiteOutputDir = os.path.join(os.getcwd(), "output")
    if not os.path.exists(svConfig.svSuiteOutputDir):
        os.makedirs(svConfig.svSuiteOutputDir)
        
    

    task = SVSuiteTask(svConfig)
    exitStatus = 0
    print task
    try:
        task()
    except SVSuiteError, ex:
        msg = "Error running SVSuite:\n"
        msg += str(ex)
        print msg
        exitStatus = ex.exitStatus
    print "RuntimeSVSuite Finished: %s" % exitStatus
    return exitStatus


if __name__ == '__main__':
    print "RuntimeSVSuite started..."
    exitStatus = main()
    if exitStatus:
        handle = open("exit.status", "w")
        handle.write(str(exitStatus))
        handle.close()
    sys.exit(exitStatus)
