#!/usr/bin/env python
"""
_RuntimeLogCollater_

Runtime script that collects log archives from previous jobs
and copies them to a central location for long term storage.

The LFN and SE of the logfile tarball is added to the FrameworkJobReport
for the job.


"""

import sys
import os
import re
import time

from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageInMgr import StageInMgr
from StageOut.DeleteMgr import DeleteMgr
from StageOut.StageOutError import StageOutInitError, StageOutFailure

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.MergeReports import mergeReports

from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery

#TODO: Add function to create failure fjr


class LogCollectorMgr:
    """
    class to do archive and copy to central location
    """
    
    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.state.loadJobSpecNode()

        self.config = self.state.configurationDict()
        
        #self.inputTasks = self.config.get("InputTasks", [])
        #self.inputReport = getTaskState(self.[-1]).getJobReport()
        #self.logsToCollect = self.getLogstoArchive()
        self.parseInputConfig()

        self.doStageOut = True 
        doingStageOut = self.config.get("DoStageOut", [])
        if len(doingStageOut) > 0:
            control = doingStageOut[-1]
            if control == "False":
                self.doStageOut = False
        

        self.workflowSpecId = self.config['WorkflowSpecID'][0]
        self.jobSpecId = self.state.jobSpecNode.jobName
        
        #create FrameworkJobReport
        self.report = FwkJobReport()
        self.report.name = "logArchive"
        self.report.jobSpecId = self.jobSpecId
        self.report.jobType = self.state.jobSpecNode.jobType
        self.report.workflowSpecId = self.workflowSpecId


    def parseInputConfig(self):
        """
        parse the input configuration
        """
        handler = IMProvHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        parser.feed(self.state.jobSpecNode.configuration)
        top = handler._ParentDoc
        
        # get wf and se and lfnBase
        wfQ = IMProvQuery("/LogCollectorConfig/wf[text()]")
        self.origWf = wfQ(top)[0]
        seQ = IMProvQuery("/LogCollectorConfig/se[text()]")
        self.se = seQ(top)[0]
        lfnBaseQ = IMProvQuery("/LogCollectorConfig/lfnBase[text()]")
        self.lfnBase = lfnBaseQ(top)[0]
        
        # get log lfns
        logQ = IMProvQuery("/LogCollectorConfig/LogsToCollect/lfn[text()]")
        self.logsToCollect = logQ(top)
        
        # get StageOut override
        commandQ = IMProvQuery("/LogCollectorConfig/Override/command[text()]")
        optionQ = IMProvQuery("/LogCollectorConfig/Override/option[text()]")
        seNameQ = IMProvQuery("/LogCollectorConfig/Override/se-name[text()]")
        lfnPrefixQ = IMProvQuery("/LogCollectorConfig/Override/lfn-prefix[text()]")
        
        # are we overriding?
        self.overrideParams = {}
        if commandQ(top):
            #overrideConf = self.config['StageOutParameters']['Override']
            self.overrideParams = {
                    "command" : None,
                    "option" : None,
                    "se-name" : None,
                    "lfn-prefix" : None,
                    }
    
            try:
                self.overrideParams['command'] = commandQ(top)[0]
                self.overrideParams['se-name'] = seNameQ(top)[0]
                self.overrideParams['lfn-prefix'] = lfnPrefixQ(top)[0]
            except StandardError, ex:
                msg = "Unable to extract Override parameters from config:\n"
                msg += str(ex)
                raise StageOutInitError(msg)
            
            option = optionQ(top)    
            if option:
                self.overrideParams['option'] = option[-1]
            else:
                self.overrideParams['option'] = ""
        
        
        
        return

    def createArchive(self, logs):
        """
        collect local file in tar file
        """
        
        #tar
        #tarName = "%s-%s-%s-Logs.tgz" % (self.origWf, self.se, int(time.time()))
        # currently causes problem - stageout code deosnt like se in pfn - fails verify
        tarName = "%s-%s-Logs.tgz" % (self.origWf, int(time.time()))
        tarfile = os.path.join(os.getcwd(), self.origWf)    #self.workflowSpecId
        if not os.path.exists(tarfile):
            os.makedirs(tarfile)
        
        for item in logs:
            src = item['PFN']
            #src = os.path.join(os.getcwd(), item)            
            #if not os.path.exists(src):
            #    print "File not found %s" % src
            #    continue
            print "Archiving File: %s" % src
            command = "/bin/mv %s %s" % (src, tarfile)
            os.system(command)
        
        tarComm = " tar -zcf %s %s" % (tarName, os.path.basename(tarfile))
        os.system(tarComm)
        
        return os.path.join(os.getcwd(), tarName)


    def __call__(self):
        """
        copy logs to local file system, tar, stage out to storage 
        and delete originals
        """
        
        #first copy logs locally
        logs = []
        fileInfo = {
            'LFN' : None,
            'PFN' : None,
            'SEName' : None,
            'GUID' : None,
            }
        try:
            stagein = StageInMgr()
            stageout = StageOutMgr(**self.overrideParams)
            delete = DeleteMgr()
        except StandardError, ex:
            msg = "Unable to load StageIn/Out/Delete Impl: %s" % str(ex)
            print msg
            self.report.exitCode = 60314
            self.report.status = "Failed"
            newError = self.report.addError(60314, "StageOutError")
            newError['Description'] = msg
            self.saveFjr()
            return
            
        #for log, se in self.logsToCollect.items():
        for log in self.logsToCollect:
            file = fileInfo
            file['LFN'] = log
            try:
                file = stagein(**file)
                logs.append(file)
            except StageOutFailure, ex:
                msg = "Unable to StageIn %s" % file['LFN']
                print msg
                self.report.addSkippedFile(file['PFN'], file['LFN'])
            
        if not logs:
            print "No logs collected"
            msg = "No logs collected\n" + msg
            self.report.exitCode = 60312
            self.report.status = "Failed"
            newError = self.report.addError(60312, "StageInError")
            newError['Description'] = msg
            self.saveFjr()
            return
            
        tarPFN = self.createArchive(logs)
        
        # now stage out tar file
        fileInfo = {
            'LFN' : "%s/%s" % (self.lfnBase, os.path.basename(tarPFN)),
            'PFN' : tarPFN,
            'SEName' : None,
            'GUID' : None,
            }
        
        try:
            fileInfo = stageout(**fileInfo)
            exitCode = 0
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print msg
            self.report.exitCode = 60314
            self.report.status = "Failed"
            #self.report.addError(60312, "StageOutError")
            self.saveFjr()
            return
        
        # delete file - ignore failures
        if exitCode == 0:
            for file in logs:
                try:
                    delete(**file)
                    #exitCode = 0    #ignore error here
                except Exception, ex:
                    msg = "Unable to delete log:\n"
                    msg += str(ex)
                    print msg
                    #exitCode = 60312    #ignore error here
        
        # write successful fjr and merge with top level
        self.report.exitCode = exitCode
        if exitCode == 0 :
            self.report.status = "Success"
            self.report.addLogFile(fileInfo['LFN'], fileInfo['SEName'])
        else:
            # at the moment do nothing with failures
            self.report.status = "Failed"
        
        self.saveFjr()
        return
            
            
    def saveFjr(self):
        """
        create fjr for process
        """
        self.report.write("./FrameworkJobReport.xml")
        
        #  //
        # // Ensure this report gets added to the job-wide report
        #//
        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],"FrameworkJobReport.xml")
        newReport = os.path.join(os.getcwd(), "FrameworkJobReport.xml")
        mergeReports(toplevelReport, newReport)


if __name__ == "__main__":
    import StageOut.Impl
    mgr = LogCollectorMgr()
    mgr()

