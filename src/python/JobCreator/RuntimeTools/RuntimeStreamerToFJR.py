#!/usr/bin/env python
"""
_RuntimeStreamerToFJR_

Runtime Script to add produced streamers to job report

"""

import os

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.UUID import makeUUID


def importProcess():
    """
    _importAndBackupProcess_
    
    Try to import the process object for the job,
    which is contained in PSet.py
    """
    try:
        from PSet import process
    except ImportError, ex:
        msg = "Failed to import PSet module containing cmsRun Config\n"
        msg += str(ex)
        raise RuntimeError, msg

    print "PSet.py imported"
    
    return process


def processFrameworkJobReport():
    """
    _processFrameworkJobReport_

    Read the job report and insert external information such as datasets
    for each file entry.

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()        
    state.loadJobSpecNode()
    state.jobSpecNode.loadConfiguration()
    state.dumpJobReport()

    # first try to load job report, abort if we can't
    try:
        state.loadJobReport()
    except Exception, ex:
        msg = "RuntimeStreamerToFJR: Error Reading JobReport: %s" % str(ex)
        raise RuntimeError, msg
    
    report = state.getJobReport()
    exitCode = state.getExitStatus()
    if exitCode != 0:
        msg = "RuntimeStreamerToFJR: non-zero exit code or no exit code"
        raise RuntimeError, msg

    process = importProcess()

    streamerFiles = {}

    # look at output modules (configuration)
##    for outputModuleName, outputModule in process.outputModules.items():
##        streamerFiles[outputModuleName] = {
##            'indexFileName' : outputModule.indexFileName
##            }

    # look at output modules (JobSpecNode)
    for outputModuleName, outputModule in state.jobSpecNode.cfgInterface.outputModules.items():
        jobReportFile = report.newFile()

        jobReportFile['LFN'] = "%s/%s.dat" % (outputModule['LFNBase'], makeUUID())
        jobReportFile['PFN'] = outputModule['fileName']
        jobReportFile['Catalog'] = None
        jobReportFile['ModuleLabel'] = outputModuleName
        jobReportFile['DisableGUID'] = "True"
        jobReportFile['Branches'] = None
        jobReportFile['OutputModuleClass'] = "EventStreamFileWriter"

        
        jobReportFile['TotalEvents'] = 1

        jobReportFile['DataType'] = "MC"
        jobReportFile['BranchHash'] = None
        jobReportFile['Inputs'] = None

        jobReportFile.addRunAndLumi(process.source.firstRun.value(),
                                    process.source.firstLuminosityBlock.value())

        # this is extra
        # should be a check on OutputModuleClass later really
        jobReportFile['FileType'] = 'STREAMER'

    # write out updated report (overwrites existing one)
    state.saveJobReport()

    print "Streamers added to job report"
    
    return


if __name__ == '__main__':
    inputReport = "./FrameworkJobReport.xml"
    if not os.path.exists(inputReport):
        msg = "ERROR: FrameworkJobReport.xml not found in directory:\n"
        msg += os.getcwd()
        msg += "\nA diagnostic report will be generated...\n"
        print msg
    inputRunResDB = "./RunResDB.xml"
    if not os.path.exists(inputRunResDB):
        msg = "ERROR: RunResDB.xml not found in directory:\n"
        msg += os.getcwd()
        msg += "\nCannot proceed with processing report\n"
        raise RuntimeError, msg
    
    processFrameworkJobReport()
