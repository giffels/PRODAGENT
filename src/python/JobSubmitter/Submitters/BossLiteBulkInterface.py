#!/usr/bin/env python
"""
python

BossLite interaction base class - should not be used directly.

"""

__revision__ = "$Id: BossLiteBulkInterface.py,v 1.40 2009/01/15 10:08:32 gcodispo Exp $"
__version__ = "$Revision: 1.40 $"

import os
import logging
import time

from JobSubmitter.Submitters.BulkSubmitterInterface \
     import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

# from ProdAgentCore.Configuration import loadProdAgentConfiguration
# from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ProdAgentException import ProdAgentException
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo #, extractDashboardID
from ProdCommon.MCPayloads.JobSpec import JobSpec

# Blite API import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job
# from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

class BossLiteBulkInterface(BulkSubmitterInterface):
    """

    Base class for GLITE bulk submission should not be used
    directly but one of its inherited classes.

    """

    # selected BossLite scheduler
    scheduler = ''

    #  //
    # // Main executable script for job: tarball unpaker
    #//
    bulkUnpackerScript = \
"""

echo \"This Job Using Spec: $JOB_SPEC_NAME\"
tar -zxf $BULK_SPEC_NAME

echo "===Available JobSpecs:==="
/bin/ls `pwd`/BulkSpecs
echo "========================="

JOB_SPEC_FILE="`pwd`/BulkSpecs/$JOB_SPEC_NAME-JobSpec.xml"

"""

    scriptEnd = \
"""
PROCEED_WITH_SPEC=0

if [ -e "$JOB_SPEC_FILE" ]; then
   echo "Found Job Spec File: $JOB_SPEC_FILE"
   PROCEED_WITH_SPEC=1
else
   echo "Job Spec File Not Found: $JOB_SPEC_NAME"
   PROCEED_WITH_SPEC=0
fi

if [ $PROCEED_WITH_SPEC != 1 ]; then
   echo "Unable to proceed without JobSpec File"
   cat > FrameworkJobReport.xml <<EOF
<FrameworkJobReport JobSpecID="$JOB_SPEC_NAME" Status="Failed">
<FrameworkError ExitStatus="60998" Type="MissingJobSpecFile">
<ExitCode Value="60998"/>
   hostname="`hostname -f`"
   jobspecfile="$JOB_SPEC_FILE"
   available_specs="`/bin/ls ./BulkSpecs 2> /dev/null`"
</FrameworkError>
</FrameworkJobReport>
EOF
   exit 60998
fi

"""

    #  //
    # // Main executable script for job: missing fjr handling
    #//
    missingRepScript = \
                     """

if [ -e FrameworkJobReport.xml ]; then
   cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR
   echo "FrameworkJobReport exists for job: $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml"
else
   echo "ERROR: No FrameworkJobReport produced by job!!!"
   echo "Generating failure report..."
   cat > FrameworkJobReport.xml <<EOF
<FrameworkJobReport JobSpecID="$JOB_SPEC_NAME" Status="Failed">
<FrameworkError ExitStatus="60997" Type="JobReportMissing">
   <ExitCode Value="60998"/>
   hostname="`hostname -f`"
   jobspecfile="$JOB_SPEC_FILE"
</FrameworkError>
</FrameworkJobReport>
EOF

   /bin/cp -f ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR
   exit 60997
fi

"""

    def __init__(self):
        """
        __init__

        overload to add further internal members
        """

        BulkSubmitterInterface.__init__( self  )
        self.bossLiteSession = None
        self.bossTask = None
        self.bulkSize = 300
        self.failedSubmission = None
        self.jobInputFiles = None
        self.mainJobSpecName = None
        self.mainSandbox = None
        self.singleSpecName = None
        self.specSandboxName = None
        self.usingDashboard = None
        self.workflowName = None
        self.workingDir = None
        self.taskName = None


    def doSubmit(self):
        """r
        __doSubmit__

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """

        # // basic check
        logging.debug("<<<<<<<<<<<<<<<<<BossLiteBulkSubmitter>>>>>>>>>>>>>>..")
        if not self.primarySpecInstance.parameters.has_key('BulkInputSandbox'):
            msg = "There is no BulkInputSandbox defined in the JobSpec." + \
                  "Submission cant go on..."
            logging.error(msg)
            return

        # // check for dashboard usage
        self.usingDashboard = {'use' : 'True', \
                               'address' : 'cms-pamon.cern.ch', \
                               'port' : '8884'}
        try:
            dashboardCfg = self.pluginConfig.get('Dashboard', {})
            self.usingDashboard['use'] = dashboardCfg.get(
                "UseDashboard", "False"
                )
            self.usingDashboard['address'] = dashboardCfg.get(
                "DestinationHost"
                )
            self.usingDashboard['port'] = dashboardCfg.get("DestinationPort")
            logging.debug("dashboardCfg = " + str(self.usingDashboard) )
        except:
            logging.info("No Dashboard section in SubmitterPluginConfig")

        # // reset some internal variables
        self.bossTask = None
        self.failedSubmission = []
        self.singleSpecName = None
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        self.bulkSize = int(self.pluginConfig['GLITE'].get('BulkSize', 300))

        # // specific submission parameters
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.workingDir = os.path.dirname(self.mainSandbox)
        self.taskName = self.workflowName + time.strftime('_%Y%m%d_%H%M%S')
        logging.debug("workingDir = %s" % self.workingDir)

        # //  Build scheduler configuration
        schedSession, submissionAttrs = self.buildScheduler()

        # // Handle submission
        if self.isBulk:
            # try loading the task
            ### try :
            ###     self.bossTask = self.bossLiteSession.loadTaskByName(
            ###         self.mainJobSpecName
            ###         )
            ###     if self.bossTask is not None:
            ###         self.taskName = self.mainJobSpecName
            ###         self.bossTask = None
            ###     else:
            ###         self.taskName = self.mainJobSpecName + '_1'
            ###         
            ### except TaskError, ex:
            ###     self.taskName = self.mainJobSpecName + '_1'

            # create task instance
            self.prepareSubmission()

            # now submit!!!
            self.submitJobs( schedSession, submissionAttrs )

        # // Handle single job submission and reSubmission 
        else:
            # loading the job
            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])
            self.singleSpecName = \
                 self.singleSpecName[:self.singleSpecName.find('-JobSpec.xml')]
            logging.info("singleSpecName \"%s\"" % self.singleSpecName)
            try :

                # load the job if it exists in the bl tables (resubmission)
                bossJob = self.bossLiteSession.loadLastJobByName(
                    self.singleSpecName
                    )

                # is there any job?
                if bossJob is None:
                    logging.info('Job does not exists in db "%s": create it' \
                                 % self.singleSpecName)
                    # no job instance in db: create a task with a job
                    self.prepareSubmission()
                    bossJob = self.bossTask.jobs[0]

                # yes, it's there! resubmit...
                else:
                    try :
                        logging.info('Job exists in db "%s"' \
                                     % self.singleSpecName)
                        # job loaded, prepare resubmission
                        self.prepareResubmission(bossJob)
                    except BossLiteError, ex:
                        logging.error('Failed to resubmit Job "%s": %s' \
                                      % (self.singleSpecName, str(ex)) )
                        raise JSException("Failed to resubmit Job", \
                                          FailureList = self.singleSpecName)

            except JobError, ex:

                logging.error('Jobs handling failed "%s": %s' \
                              % (self.singleSpecName, str(ex)) )
                self.failedSubmission = self.toSubmit.keys()
                raise JSException("Failed handling Job", \
                                  FailureList = self.failedSubmission)

            # now submit!!!
            self.submitSingleJob( schedSession, submissionAttrs )

        # // check for not submitted and eventually raise Submission Failed
        for job in self.bossTask.jobs :
            if job.runningJob['schedulerId'] is None \
                   and job['name'] not in self.failedSubmission:
                self.failedSubmission.append( job['name'] )

        if self.failedSubmission != []:
            raise JSException("Submission Failed", \
                              FailureList = self.failedSubmission)

        return

    

    def prepareResubmission(self, bossJob):
        """
        __prepareResubmission__

        If already declared (i.e. resubmission), just submit
        """

        logging.info("Resubmitting job %s" % self.singleSpecName)

        # check for loaded job
        if bossJob is None:
            msg = 'Failed to retrieve job %s' % self.singleSpecName
            logging.error( msg )
            self.failedSubmission = self.toSubmit.keys()
            raise JSException(msg, FailureList = self.failedSubmission)

        # check if the wrapper is actually there
        executable = os.path.join(self.workingDir, bossJob['executable'] )
        if not os.path.exists( executable ):
            logging.info("missing wrapper script %s: recreating it!" \
                          % executable)
            self.makeWrapperScript( executable, "$1" )

        logging.debug( "BossLiteBulkInterface.doSubmit bossJobId = %s.%s" \
                       % (bossJob['taskId'], bossJob['jobId']) )

        # is there a job? build a task!
        logging.info( "Loading task for job resubmission..."  )
        self.bossLiteSession.getRunningInstance( bossJob )

        # close previous instance and set up the outdir
        if bossJob.runningJob['processStatus'] != 'created' \
               and bossJob.runningJob['processStatus'] is not None:

            # eventually close old instance
            if bossJob.runningJob['closed'] != 'Y' :
                bossJob.runningJob['closed'] = 'Y'
                self.bossLiteSession.updateDB( bossJob.runningJob )
                logging.warning(
                    "Previous RunningInstance %s.%s.%s not closed. Forcing" % \
                    ( bossJob['taskId'], bossJob['jobId'], \
                      bossJob.runningJob['submission'] ) )

            logging.warning(
                "Previous RunningInstance %s.%s.%s " % \
                ( bossJob['taskId'], bossJob['jobId'], \
                  bossJob.runningJob['submission'] ) )
            # creating new RunningInstance
            self.bossLiteSession.getNewRunningInstance( bossJob )
            bossJob.runningJob['outputDirectory'] = os.path.join(
                self.singleSpecName, time.strftime('%Y%m%d_%H%M%S') )
            self.bossLiteSession.updateDB( bossJob )
            logging.warning(
                "next RunningInstance %s.%s.%s " % \
                ( bossJob['taskId'], bossJob['jobId'], \
                  bossJob.runningJob['submission'] ) )

        # load the task ans append the job
        self.bossTask = self.bossLiteSession.loadTask(
            bossJob['taskId'], bossJob['jobId'] )

        # still no task? Something bad happened
        if self.bossTask is not None :
            logging.debug( "BossLiteBulkInterface: Submit bossTask = %s" \
                           % self.bossTask['id'] )
        else:
            self.failedSubmission = self.toSubmit.keys()
            raise JSException("Failed to find Job", \
                              FailureList = self.failedSubmission)



    def prepareSubmission(self):
        """
        __prepareSubmission__

        register task in the BossLite tables
        """

        # // Build a list of input files for every job
        #//  For multiple bulk jobs there will be a tar of specs
        if self.isBulk:
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jobInputFiles = [
                self.mainSandbox, 
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                ]
            executable = self.workflowName + '-submit'
        else:
            self.jobInputFiles = [ self.specFiles[self.mainJobSpecName],
                                   self.mainSandbox ]
            executable = self.singleSpecName + '-submit'
            
        # // generate unique wrapper script
        logging.debug("mainJobSpecName = \"%s\"" % self.mainJobSpecName)
        executablePath = "%s/%s" % (self.workingDir, executable)
        logging.debug("makeWrapperScript = %s" % executablePath)
        self.makeWrapperScript( executablePath, "$1" )

        inpSandbox = ','.join( self.jobInputFiles )
        logging.debug("Declaring to BOSS")

        wrapperName = "%s/%s" % (self.workingDir, self.mainJobSpecName)

        # insert task
        try :

            self.bossTask = Task()
            self.bossTask['name'] = self.taskName
            self.bossTask['globalSandbox'] = executablePath + ',' + inpSandbox
            self.bossTask['jobType'] = \
                                 self.primarySpecInstance.parameters['JobType']
            self.bossLiteSession.saveTask( self.bossTask )

        except BossLiteError, ex:
            self.failedSubmission = self.toSubmit.keys()
            raise JSException(str(ex), FailureList = self.failedSubmission)

        # insert jobs
        try :
            outdir = time.strftime('%Y%m%d_%H%M%S')
            for jobSpecId, jobCacheDir in self.toSubmit.items():
                if len(jobSpecId) == 0 :#or jobSpecId in jobSpecUsedList :
                    continue
                job = Job()
                job['name'] = jobSpecId
                job['arguments'] =  jobSpecId
                job['standardOutput'] =  jobSpecId + '.log'
                job['standardError']  =  jobSpecId + '.log'
                job['executable']     =  executable
                job['outputFiles'] = [ jobSpecId + '.log', \
                                       jobSpecId + '.tgz', \
                                       'FrameworkJobReport.xml' ]
                self.bossLiteSession.getNewRunningInstance( job )
                job.runningJob['outputDirectory'] = \
                                            os.path.join( jobCacheDir, outdir )
                self.bossTask.addJob( job )
            self.bossLiteSession.updateDB( self.bossTask )
            logging.info( "Successfully Created task %s with %d jobs" % \
                          ( self.bossTask['id'], len(self.bossTask.jobs) ) )

        except BossLiteError, ex:
            self.failedSubmission = self.toSubmit.keys()
            raise JSException(str(ex), FailureList = self.failedSubmission)

        return


    def makeWrapperScript(self, filename, jobName):
        """
        _makeWrapperScript_

        Make the main executable script for condor to execute the
        job

        """

        # // Generate main executable script for job
        script = ["#!/bin/sh\n\n"]
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("JOB_SPEC_NAME=%s\n" % jobName)
        if self.isBulk:
            script.append("BULK_SPEC_NAME=\"%s\"\n" % self.specSandboxName )
            script.append( self.bulkUnpackerScript )
            ### script.extend(bulkUnpackerScript(self.specSandboxName))
        else:
            script.append(
                "JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s-JobSpec.xml\n" \
                          % self.singleSpecName
                )

        script.append( self.scriptEnd )
        script.append( "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % \
                       os.path.basename(self.mainSandbox) )
        script.append("cd %s\n" % self.workflowName)
        script.append( "./run.sh $JOB_SPEC_FILE > %s.out 2> %s.err\n" \
                       % ( jobName, jobName ) )
        script.append(
            "tar cvzf $PRODAGENT_JOB_INITIALDIR/%s.tgz  %s.out %s.err\n" \
            % ( jobName, jobName, jobName )
            )

        # Handle missing FrameworkJobReport
        ### script.extend(missingJobReportCheck(jobName))
        script.append(self.missingRepScript)

        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()
        return


    def buildScheduler(self):
        """
        __buildScheduler__

        Build scheduler configuration

        """

        #  // retrieve scheduler additiona info
        # //  and eventually change the RB
        #//   according to user provided configuration files
        logging.info("Building up scheduler configuration")
        schedulerConfig = self.getSchedulerConfig()

        #  // build scheduler session, which also checks proxy validity
        # //  an exception raised will stop the submission
        try:
            schedulerCladFile = self.getSchedulerConfig()
            schedulerConfig = { 'name' : self.scheduler,
                                'config' : schedulerCladFile }
            schedSession = BossLiteAPISched( self.bossLiteSession, \
                                             schedulerConfig )

        except BossLiteError, err:
            logging.error( "########### Failed submission : %s" % str( err ) )
            self.failedSubmission = self.toSubmit.keys()
            raise JSException( "Unable to find a valid certificate", \
                               FailureList = self.failedSubmission )

        # // performing scheduler specific operations
        self.configureScheduler( schedSession )

        # // prepare extra jdl attributes
        logging.info("Preparing scheduler specific attributes")
        try:
            jobType = self.primarySpecInstance.parameters['JobType']
            submissionAttrs = self.createSchedulerAttributes(jobType)
        except Exception, ex:
            msg = "Unable to build scheduler specific attributes"
            logging.error( msg )
            self.failedSubmission = self.toSubmit.keys()
            raise JSException( msg, FailureList = self.failedSubmission )

        # return scheduler session and configuration
        return ( schedSession, submissionAttrs )


    def writeJdl(self, schedSession, submissionAttrs):
        """
        __writeJdl__

        writes jdl in a file
        """

        # do we need to write the file?
        try :
            schedulercladfile = "%s/%s_scheduler.clad" % \
                                (self.workingDir , self.mainJobSpecName )
            declareClad = open(schedulercladfile,"w")
            declareClad.write( schedSession.jobDescription( self.bossTask, \
                                               requirements=submissionAttrs ) )
            declareClad.close()
        except:
            logging.error( "unable to build scheduler specific requirements" )


    def submitSingleJob(self, schedSession, submissionAttrs ):
        """
        __submitJob__

        submit for single job
        """
        
        # // Executing BOSS Submit command
        logging.info("Submitting job %s" % self.singleSpecName)
        try :
            self.bossTask = schedSession.submit( self.bossTask, \
                                                 requirements=submissionAttrs )
        except BossLiteError, err:
            logging.error( "########### Failed submission : %s" % \
                           str(schedSession.getLogger()) )


    def submitJobs(self, schedSession, submissionAttrs ):
        """
        __submitJobs__

        submit for range of jobs
        """

        offset = 0
        loop = True

        while loop :

            logging.debug("Max bulk size: %s:%s " % \
                          (str( offset ), str( offset + self.bulkSize) ) )

            task = self.bossLiteSession.load( self.bossTask['id'], \
                                              limit=self.bulkSize, \
                                              offset=offset )

            # exit if no more jobs to query
            if task.jobs == [] :
                loop = False
                break
            else :
                offset += self.bulkSize

            # // Executing BOSS Submit command
            try :
                logging.info ("Submit jobs from %s to %s" % \
                               (task.jobs[0]['jobId'], task.jobs[-1]['jobId']))
                schedSession.submit(task, requirements=submissionAttrs)
            except BossLiteError, err:
                self.failedSubmission.extend( self.toSubmit.keys() )
                logging.error( "########### Failed submission : %s" % \
                               str(schedSession.getLogger()) )

        self.bossTask = self.bossLiteSession.load(self.bossTask['id'])
        



    def publishSubmitToDashboard( self ):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        """

        if  self.usingDashboard['use'] != 'True':
            return

        appData = str(self.applicationVersions)
        appData = appData.replace("[", "")
        appData = appData.replace("]", "")
        whitelist = str(self.whitelist)
        whitelist = whitelist.replace("[", "")
        whitelist = whitelist.replace("]", "")

        for job in self.bossTask.jobs :
            if job.runningJob['schedulerId'] is None \
                   or job['name'] in self.failedSubmission:
                continue

            # compose DashboardInfo.xml path
            dashboardInfo = DashboardInfo()
            jobdir = self.toSubmit[ job['name'] ]
            dashboardInfoFile = os.path.join(jobdir, "DashboardInfo.xml" )

            if os.path.exists(dashboardInfoFile):

                try:
                    # it exists, get dashboard information
                    dashboardInfo.read(dashboardInfoFile)

                except StandardError, msg:
                    # it does not work, abandon
                    logging.error("Reading dashboardInfoFile " + \
                                  dashboardInfoFile + " failed (jobId=" \
                                  + str(job['jobId']) + ")\n" + str(msg))

            # assign job dashboard id
            dashboardInfo.task, dashboardInfo.job = \
                                self.generateDashboardID( job, jobdir )

            # job basic information
            dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
            dashboardInfo['Scheduler'] = self.__class__.__name__
            dashboardInfo['ApplicationVersion'] = appData
            dashboardInfo['TargetCE'] = whitelist
            dashboardInfo['GridJobID'] = job.runningJob['schedulerId']
            dashboardInfo['SubTimeStamp'] = job.runningJob['submissionTime']
            dashboardInfo['RBname'] = job.runningJob['service']
            dashboardInfo.addDestination(
                self.usingDashboard['address'], self.usingDashboard['port']
                )

            # update dashboard info file
            try:
                dashboardInfo.write( dashboardInfoFile )
                logging.info("Created dashboardInfoFile " + dashboardInfoFile )
            except Exception, ex:
                logging.error("Error writing %s" % dashboardInfoFile)

            # publish to Dashboard
            try:
                dashboardInfo.publish(1)
                logging.info("Published to dashboard: %s" % str(dashboardInfo))
            except Exception, ex:
                logging.error("Error publishing to dashboard: %s" % str(ex))

        return



    def generateDashboardID(self, job, jobdir):
        """
        _generateDashboardID_

        Generate a global job ID for the dashboard

        """
        
        jobSpecFile = os.path.join(jobdir, "%s-JobSpec.xml" % job['name'])
        jobSpec = JobSpec()
        jobSpec.load(jobSpecFile)

        prodAgentName = jobSpec.parameters['ProdAgentName']
        subCount = jobSpec.parameters.get('SubmissionCount', 0)
        jobSpecId = jobSpec.parameters['JobName']
        workflowName = jobSpec.payload.workflow
        
        jobName = "ProdAgent_%s_%s_%s_%s" % (
            prodAgentName, \
            jobSpecId.replace("_", "-"), \
            subCount, \
            job.runningJob['schedulerId']
            )

        taskName = "ProdAgent_%s_%s" % (
            workflowName.replace("_", "-"), \
            prodAgentName
            )

        return taskName, jobName


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)


    def createSchedulerAttributes(self, jobType):
        """
        _createSchedulerAttributes_

        create the scheduler attributes
        Specific implementation in the Scheduler specific part
                 (e.g. BlGLiteBulkSubmitter)
        """

        return ''


    def getSchedulerConfig(self) :
        """
        _getSchedulerConfig_

        retrieve configuration info for the BossLite scheduler
        Specific implementation in the Scheduler specific part
                 (e.g. BlGLiteBulkSubmitter)
        """

        return ''


    def configureScheduler(self, schedSession) :
        """
        _configureScheduler_

        perform any scheduler specific operation
        Specific implementation in the Scheduler specific part
                 (e.g. BlGLiteBulkSubmitter)
        """

        return ''

