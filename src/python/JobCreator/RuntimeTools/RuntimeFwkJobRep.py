#!/usr/bin/env python
"""
_RuntimeFwkJobRep_

Runtime tool for processing the Framework Job Report produced
by CMSSW executables.


"""

import os
import socket

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

    
    
    #  //
    # // Workaround bad format Data nodes for GeneratorInfo in 
    #//  121
    
    reportLines = []
    handle = open(state.jobReport, 'r')
    for line in handle.readlines():
        if line.find("<Data ") > -1:
            if line.find("/>") == -1:
                line = line.replace(">", "/>")
        reportLines.append(line)
        
    rewrite = open(state.jobReport, 'w')
    rewrite.writelines(reportLines)
    rewrite.close()
    
        

        
    state.dumpJobReport()
    
    

    try:
        state.loadJobReport()
    except Exception, ex:
        #  //
        # // Error reading report ==> it is corrupt.
        #//  Setting it to None means that it will be converted into
        #  //a diagnostic report in the following code
        # //
        #//
        print "Error Reading JobReport:"
        print str(ex)
        state._JobReport = None

    #  //
    # // If no report file is found, we create an empty one to
    #//  make sure we report the failure implied by the missing report
    if state.getJobReport() == None:
        print "Generating Job Report by hand..."
        state._JobReport = FwkJobReport()
        
        
    
    
    
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
    # // Include site details in job report
    #//
    report = state.getJobReport()
    siteName = "Unknown"
    hostName = socket.gethostname()
    seName = "Unknown"
    state.loadSiteConfig()
    siteCfg = state.getSiteConfig()
    if siteCfg != None:
        siteName = siteCfg.siteName
        if siteCfg.localStageOut.get('se-name', None) != None:
            seName = siteCfg.localStageOut['se-name']

            
    report.siteDetails['SiteName'] = siteName
    report.siteDetails['HostName'] = hostName
    report.siteDetails['se-name'] = seName    
    
    #  //
    # // If available, include basic start/stop times in job report
    #// 
    if os.path.exists("start.time"):
        report.timing['AppStartTime'] = file("start.time").read().strip()
    if os.path.exists("end.time"):
        report.timing['AppEndTime'] = file("end.time").read().strip()
        
        
    
    #  //
    # // write out updated report
    #//
    
    report.status = reportStatus
    report.workflowSpecId = state.taskAttrs['WorkflowSpecID']
    report.jobSpecId = state.taskAttrs['JobSpecID']
    report.jobType = state.taskAttrs['JobType']
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
    
