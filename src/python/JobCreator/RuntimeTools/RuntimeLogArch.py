#!/usr/bin/env python
"""
_RuntimeLogArch_

Runtime script that collects all logfiles within a job, builds a tarfile
and stages them out to the local SE.
The PFN/LFN of the logfile tarball is added to the FrameworkJobReport
for the job.


"""
__version__ = "$Revision: 1.15 $"
__revision__ = "$Id: RuntimeLogArch.py,v 1.15 2009/02/13 15:27:54 evansde Exp $"

import sys
import os
import re
import time

from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from StageOut.StageOutMgr import StageOutMgr
from StageOut.StoreFail import StoreFailMgr

from StageOut.StageOutError import StageOutInitError

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.MergeReports import mergeReports
from ProdCommon.FwkJobRep.MergeReports import updateReport

class LogArchMgr:


    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.state.loadJobSpecNode()

        #  //
        # // check for store fail settings
        #//
        self.workflow = WorkflowSpec()
        self.workflow.load(os.environ['PRODAGENT_WORKFLOW_SPEC'])
        self.doStoreFail = self.workflow.parameters.get("UseStoreFail", False)
        if str(self.doStoreFail).lower() == "true":
            self.doStoreFail = True

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


        # TODO: These should be pulled in from the workflow now not the config thing
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

    def wasJobFailure(self):
        """
        _wasJobFailure_

        Check wether the job is flagged as a faillure in the toplevel report
        return boolean, True if job was a failure
        """
        status = False
        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],"FrameworkJobReport.xml")
        toplevelReps = readJobReport(toplevelReps)
        for rep in toplevelReps:
            if not rep.wasSuccess():
                status = True
        return status

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

        # add job/workflow spec and fjr
        for src in (os.getenv("PRODAGENT_WORKFLOW_SPEC", ''),
                     os.getenv("PRODAGENT_JOBSPEC", ''),
                     os.path.join(os.getenv("PRODAGENT_JOB_DIR", ''), "FrameworkJobReport.xml")):
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

        runNum = runPadding = None
        runNum1 = self.state.jobSpec.parameters.get('RunNumber', None)
        runNum2 = self.state.jobSpec.parameters.get('MergeJobNumber', None)
        for run in (runNum1, runNum2):
            if run is not None:
                runNum = int(run)
                runPadding = str(runNum // 1000).zfill(4)
                break
        if runNum is None:
            # no jobNumber - use day and hope for no collisions
            runPadding = time.gmtime()[7] # what day is it?
            run = self.jobSpecId

        reqtime = self.state.jobSpec.parameters.get('RequestTimestamp', None)
        if reqtime is not None:
            reqtime = time.gmtime(int(reqtime))
        else:
            reqtime = time.gmtime()
        year, month, day = reqtime[:3]

        fileInfo = {
            'LFN' : "/store/unmerged/logs/prod/%s/%s/%s/%s/%s/%s/%s" % \
                                        (year, month, day, self.workflowSpecId,
                                         runPadding, runNum, tarName),
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
        # create sub directory for this tasks files to go in
        taskArchiveDir = os.path.join(self.tarfile, task)
        try:
            os.makedirs(taskArchiveDir)
        except OSError, ex:
            print "Error creating directory %s: %s" % (taskArchiveDir, str(ex))

        # stage out files so far to store/fail if activated
        if self.doStoreFail:
            if self.wasJobFailure():
                failLog = []
                taskState = getTaskState(task)
                report = taskState.getJobReport()
                if report != None:
                    storeFailMgr = StoreFailMgr(report)
                    failLog.extend(storeFailMgr())

                failLogFile = os.path.join(taskDir, "StoreFail.log")
                handle = open(failLogFile, 'w')
                for f in failLog:
                    handle.write("%s\n" % f)
                handle.close()



        toArchive = []
        taskContents = os.listdir(taskDir)

        for compRE in self.compRegexps:
            [ toArchive.append(x) for x in taskContents if compRE.search(x) ]

        for item in toArchive:
            src = os.path.join(taskDir, item)
            print "Archiving File: %s" % src
            command = "/bin/cp -f %s %s" % (src, taskArchiveDir)
            os.system(command)
            #self.tarfile.add(src, "%s/%s/%s" % (self.jobSpecId, task, item))

        return





if __name__ == "__main__":
    import StageOut.Impl
    mgr = LogArchMgr()
    mgr()
