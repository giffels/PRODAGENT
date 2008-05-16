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

from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from ProdCommon.FwkJobRep.MergeReports import mergeReports
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.MergeReports import combineReports
import ProdCommon.FwkJobRep.PerfLogParser as PerfReps

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
        command = "glite-brokerinfo getCE"
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
    state.loadJobSpecNode()
    state.jobSpecNode.loadConfiguration()
    state.dumpJobReport()
    
    
    badReport = False
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
        badReport = True
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
    if badReport:
        #  //
        # // Unreadable report => make sure this gets logged
        #//
        exitCode = 50115
        reportStatus = "Failed"
        
        
    if exitCode == None:
        print "WARNING: CANNOT FIND EXIT STATUS..."
        exitCode = 50116
        reportStatus = "Failed"
        
    if exitCode != 0:
        reportStatus = "Failed"

    report.status = reportStatus
    report.exitCode = exitCode
    report.workflowSpecId = state.taskAttrs['WorkflowSpecID']
    report.jobSpecId = state.taskAttrs['JobSpecID']
    report.jobType = state.taskAttrs['JobType']
    
    
    if report.name == None:
        taskName = state.taskAttrs['Name']
        report.name = taskName
        
        
    #  //
    # // filter zero event output files
    #//  TODO: Make this configurable via ProdAgent config switch
    #[ report.files.remove(x) for x in report.files if x['TotalEvents'] == 0 ]
    #  //
    # // generate sizes and checksums
    #//
    state.generateFileStats()
    
    #  //
    # // match files to datasets.
    #//
    state.assignFilesToDatasets()



    
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
    # // Add Performance Report if logfiles are Available
    #//
    stderrLog = "%s-main.sh-stderr.log" % state.taskName()
    perfReport = "PerfReport.log"
    if not os.path.exists(stderrLog):
        stderrLog = None
    if not os.path.exists(perfReport):
        perfReport = None
    PerfReps.makePerfReports(report.performance, stderrLog, perfReport)
    report.performance.getInfoOnWorker()
    
    #  //
    # // write out updated report
    #//
    
    toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")

    if state.jobSpecNode._InputLinks and os.path.exists(toplevelReport):
        #  // 
        # // Merge with report from input node, save to toplevel and locally
        #//
        inputTaskNames = [ getTaskState(x['InputNode']).taskName() \
                          for x in state.jobSpecNode._InputLinks ]
        report = combineReports(toplevelReport, inputTaskNames, report)
        report.write("./FrameworkJobReport.xml")
    else:
        #  // 
        # // Add this report to the job toplevel report
        #//  This will create the toplevel job report if it doesnt
        #  //exist, otherwise it will merge this report with whatever
        # // is in there already.
        #//
        report.write("./FrameworkJobReport.xml")
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
    
