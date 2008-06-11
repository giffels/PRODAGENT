#!/usr/bin/env python
"""
_RuntimeLogArch_

Runtime script that collects all logfiles within a job, builds a tarfile
and stages them out to the local SE.
The PFN/LFN of the logfile tarball is added to the FrameworkJobReport
for the job.


"""

import sys
import os
import re
import time

from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from StageOut.StageOutMgr import StageOutMgr

from StageOut.StageOutError import StageOutInitError

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.MergeReports import mergeReports
from ProdCommon.FwkJobRep.MergeReports import updateReport

class LogArchMgr:


    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.state.loadJobSpecNode()  

        self.config = self.state.configurationDict()
        
        self.inputTasks = self.config.get("InputTasks", [])
#        #TODO: not really sure this is correct, i would think i should
#        # take report from stageOut but that is missing if cmsRun fails
#        # what if cmsRun one is missing - do i want to generate an empty one?
#        self.inputReport = getTaskState(self.inputTasks[0]).getJobReport()
        
        # iterate over input tasks (in reverse order)
        # find first one with a fjr
        self.inputTask, self.inputReport = None, None
        for taskName in self.inputTasks[::-1]:
            task = getTaskState(taskName)
            report = task.getJobReport()
            if report is None:
                continue
            self.inputTask = task
            self.inputReport = report
            break
        
        # if got no valid fjr from previous tasks -
        # something must have gone wrong earlier - make our own
        # may need more things set here to make reports mergeable
        if self.inputReport is None:
            self.inputTask = self.state
            self.inputReport = FwkJobReport()
        
        self.regexps = self.config.get("LogMatchRegexp", [])

        self.doStageOut = True
        doingStageOut = self.config.get("DoStageOut", [])
        if len(doingStageOut) > 0:
            control = doingStageOut[-1]
            if control == "False":
                self.doStageOut = False
        

        self.workflowSpecId = self.config['WorkflowSpecID'][0]
        self.jobSpecId = self.state.jobSpecNode.jobName
        
        
        self.compRegexps = []
        for regexp in self.regexps:
            self.compRegexps.append(re.compile(regexp))


        self.override = False
        soParams = self.config.get('StageOutParameters', {})
        self.override = soParams.has_key("Override")
        self.overrideParams = {}
        
        if self.override:
            overrideConf = self.config['StageOutParameters']['Override']
            self.overrideParams = {
                "command" : None,
                "option" : None,
                "se-name" : None,
                "lfn-prefix" : None,
                }

            try:
                self.overrideParams['command'] = overrideConf['command'][0]
                self.overrideParams['se-name'] = overrideConf['se-name'][0]
                self.overrideParams['lfn-prefix'] = overrideConf['lfn-prefix'][0]
            except StandardError, ex:
                msg = "Unable to extract Override parameters from config:\n"
                msg += str(self.config['StageOutParameters'])
                raise StageOutInitError(msg)
            
            if overrideConf.has_key('option'):
                if len(overrideConf['option']) > 0:
                    self.overrideParams['option'] = overrideConf['option'][-1]
                else:
                    self.overrideParams['option'] = ""
        
    def __call__(self):
        """
        _operator()_

        tar up and xfer logfiles

        """
        #  //
        # // Create the tarfile
        #//
        tarName = "%s-%s-LogArch.tgz" % (self.jobSpecId, int(time.time()))
        self.tarfile = os.path.join(os.getcwd(), self.jobSpecId)
        if not os.path.exists(self.tarfile):
            os.makedirs(self.tarfile)
            
        # add job/workflow spec
        for spec in ("PRODAGENT_WORKFLOW_SPEC", "PRODAGENT_JOBSPEC"):
            src = os.getenv(spec, '')
            if os.path.exists(src):
                print "Archiving File: %s" % src
                command = "/bin/cp -f %s %s" % (src, self.tarfile)
                os.system(command)
            
        for task in self.inputTasks:
            self.processTask(task)
            
        tarComm = " tar -zcf %s %s" % (tarName, self.jobSpecId)
        os.system(tarComm)
        
        #  //
        # // Try to stage out log archive
        #//
        if not self.doStageOut:
            print "Stage Out of LogArchive is disabled."
            return
        print "Attempting Stage Out of LogArchive..."
        try:
            stager = StageOutMgr(**self.overrideParams)
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print msg
            return

        fileInfo = {
            'LFN' : "/store/unmerged/logs/prod/%s/%s/%s/%s/%s" % \
                                        (time.gmtime()[0], time.gmtime()[1],
                                         self.workflowSpecId, self.jobSpecId,
                                        tarName),
            'PFN' : os.path.join(os.getcwd(), tarName),
            'SEName' : None,
            'GUID' : None,
            }
        
        try:
            fileInfo = stager(**fileInfo)
            exitCode = 0
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print msg
            exitCode = 60312
            
        # exit if stageOut failed - dont propagate error to fjr
        if exitCode != 0:
            return
    
        self.inputReport.addLogFile(fileInfo['LFN'], fileInfo['SEName'])
        self.inputTask.saveJobReport()
        
        #  //
        # // Ensure this report gets added to the job-wide report
        #//
        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],"FrameworkJobReport.xml")
        updateReport(toplevelReport, self.inputReport)
        
        

    def processTask(self, task):
        """
        _processTask_

        Search for the logfiles in the named task

        """
        print  "Archiving task logs: %s" % task
        taskDir =   os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                 task)
        toArchive = []
        taskContents = os.listdir(taskDir)

        for compRE in self.compRegexps:
            [ toArchive.append(x) for x in taskContents if compRE.search(x) ]

        for item in toArchive:
            src = os.path.join(taskDir, item)
            print "Archiving File: %s" % src
            command = "/bin/cp -f %s %s" % (src, self.tarfile)
            os.system(command)
            #self.tarfile.add(src, "%s/%s/%s" % (self.jobSpecId, task, item))
            
        return


        
    

if __name__ == "__main__":
    import StageOut.Impl
    mgr = LogArchMgr()
    mgr()
