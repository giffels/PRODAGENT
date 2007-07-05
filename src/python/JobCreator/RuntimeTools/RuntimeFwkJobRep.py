#!/usr/bin/env python
"""
_RuntimeFwkJobRep_

Runtime tool for processing the Framework Job Report produced
by CMSSW executables.


"""

import os
import socket
import time
import popen2

from FwkJobRep.TaskState import TaskState
from FwkJobRep.MergeReports import mergeReports
from FwkJobRep.FwkJobReport import FwkJobReport


def getSyncCE():
    """
    _getSyncCE_

    Extract the SyncCE from GLOBUS_GRAM_JOB_CONTACT if available for OSG,
    otherwise broker info for LCG

    """
    result = socket.gethostname()

    if os.environ.has_key('GLOBUS_GRAM_JOB_CONTACT'):
        #  //
        # // OSG, Sync CE from Globus ID
        #//
        val = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        try:
            host = val.split("https://", 1)[1]
            host = host.split(":")[0]
            result = host
        except:
            pass
        return result
    if os.environ.has_key('EDG_WL_JOBID'):
        #  //
        # // LCG, Sync CE from edg command
        #//
        command = "edg-brokerinfo getCE"
        pop = popen2.Popen3(command)
        pop.wait()
        exitCode = pop.poll()
        if exitCode:
            return result 
        
        content = pop.fromchild.read()
        result = content.strip()
        return result
    return result

def getDashboardId():
    """
    _getDashboardId_
    
    Extract dashboard id from environemt variable
    """
    return os.environ.get("PRODAGENT_DASHBOARD_ID", "Unknown")


def processFrameworkJobReport():
    """
    _processFrameworkJobReport_

    Read the job report and insert external information such as datasets
    for each file entry.

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()        
    
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
        

    report = state.getJobReport()
    exitCode = state.getExitStatus()
    reportStatus = "Success"
    if exitCode == None:
        print "WARNING: CANNOT FIND EXIT STATUS..."
        reportStatus = "Unknown"
    else:
        if exitCode != 0:
            reportStatus = "Failed"

    report.status = reportStatus
    report.workflowSpecId = state.taskAttrs['WorkflowSpecID']
    report.jobSpecId = state.taskAttrs['JobSpecID']
    report.jobType = state.taskAttrs['JobType']
    if exitCode != None:
        report.exitCode = exitCode
    if report.name == None:
        taskName = state.taskAttrs['Name']
        report.name = taskName
        
    
    
    #  //
    # // match files to datasets.
    #//
    state.assignFilesToDatasets()
    state.generateFileStats()

    
    #  //
    # // Include site details in job report
    #//
    siteName = "Unknown"
    hostName = socket.gethostname()
    seName = "Unknown"
    ceName = getSyncCE()
    state.loadSiteConfig()
    siteCfg = state.getSiteConfig()
    if siteCfg != None:
        siteName = siteCfg.siteName
        if siteCfg.localStageOut.get('se-name', None) != None:
            seName = siteCfg.localStageOut['se-name']

            
    report.siteDetails['SiteName'] = siteName
    report.siteDetails['HostName'] = hostName
    report.siteDetails['se-name'] = seName
    report.siteDetails['ce-name'] = ceName    
    
    #  //
    # // If available, include basic start/stop times in job report
    #// 
    if os.path.exists("start.time"):
        report.timing['AppStartTime'] = file("start.time").read().strip()
    if os.path.exists("end.time"):
        report.timing['AppEndTime'] = file("end.time").read().strip()
        
    #  //    
    # // add dashboard id
    #//
    report.dashboardId = getDashboardId()
    
    #  //
    # // write out updated report
    #//
    
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
    
