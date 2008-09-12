#!/usr/bin/env python
"""
_RuntimeFwkJobRep_

Runtime tool for processing the Framework Job Report produced
by CMSSW executables.


"""

import os

from FwkJobRep.TaskState import TaskState
from FwkJobRep.MergeReports import mergeReports
from FwkJobRep.FwkJobReport import FwkJobReport

def processFrameworkJobReport():
    """
    _processFrameworkJobReport_

    Read the job report and insert external information such as datasets
    for each file entry.

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()

    try:
        state.loadJobReport()
    except:
        #  //
        # // Error reading report ==> it is corrupt.
        #//  Setting it to None means that it will be converted into
        #  //a diagnostic report in the following code
        # //
        #//
        state._JobReport = None

    #  //
    # // If no report file is found, we create an empty one to
    #//  make sure we report the failure implied by the missing report
    if state.getJobReport() == None:
        state._JobReport = FwkJobReport()
        diagnostic = state._JobReport.newMessage()
        diagnostic['Message'].append("Job Report Not created by Exe\n")
        diagnostic['Message'].append(
            "This Report Generated by RuntimeFwkJobReport.py\n")
        
    
    
    #  //
    # // match files to datasets.
    #//
    state.assignFilesToDatasets()
    state.generateFileStats()

    #  //
    # // Check Exit Status file, set exit code in job report
    #//  and set the status of the report accordingly
    exitCode = state.getExitStatus()
    reportStatus = "Success"
    if exitCode == None:
        print "WARNING: CANNOT FIND EXIT STATUS..."
        reportStatus = "Unknown"
    else:
        if exitCode != 0:
            reportStatus = "Failed"
        
    
    #  //
    # // write out updated report
    #//
    report = state.getJobReport()
    report.status = reportStatus
    report.workflowSpecId = state.taskAttrs['WorkflowSpecID']
    report.jobSpecId = state.taskAttrs['JobSpecID']
    if exitCode != None:
        report.exitCode = exitCode
    if report.name == None:
        taskName = state.taskAttrs['Name']
        report.name = taskName
    report.write("./FrameworkJobReport.xml")


    #  // 
    # // Add this report to the job toplevel report
    #//  This will create the toplevel job report if it doesnt
    #  //exist, otherwise it will merge this report with whatever
    # // is in there already.
    #//
    toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")
    newReport = os.path.join(os.getcwd(), "FrameworkJobReport.xml")
    mergeReports(toplevelReport, newReport)
    
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
    
