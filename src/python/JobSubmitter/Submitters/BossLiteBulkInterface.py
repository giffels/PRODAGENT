#!/usr/bin/env python
"""
python

BossLite interaction base class - should not be used directly.

"""

__revision__ = "$Id: BossLiteBulkInterface.py,v 1.3 2008/06/04 14:36:33 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import os, time
import logging


from JobSubmitter.Submitters.BulkSubmitterInterface \
     import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ProdAgentException import ProdAgentException
from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

# Blite API import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import SchedulerError

class BossLiteBulkInterface(BulkSubmitterInterface):
    """

    Base class for GLITE bulk submission should not be used
    directly but one of its inherited classes.

    """

    # selected BossLite scheduler
    scheduler = ''

    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """
        logging.debug("<<<<<<<<<<<<<<<<<BossLiteBulkSubmitter>>>>>>>>>>>>>>..")

        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        if not self.primarySpecInstance.parameters.has_key('BulkInputSandbox'):
            msg = "There is no BulkInputSandbox defined in the JobSpec. Submission cant go on..."
            logging.error(msg)
            return
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.singleSpecName = None
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        self.bossJob = None
        self.bossTask = None
        self.submittedJobs = {}
        self.failedSubmission = []
        self.jobInputFiles = []
        
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles.append(self.mainSandbox)
        
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jobInputFiles.append(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'])

        #  //
        # // check for dashboard usage
        #//
        self.usingDashboard = {'use' : 'True', \
                               'address' : 'cms-jobmon.cern.ch', \
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
            logging.debug("dashboardCfg = " + self.usingDashboard.__str__() )
        except:
            logging.info("No Dashboard section in SubmitterPluginConfig")
        
        self.workingDir = os.path.dirname(self.mainSandbox)
        logging.debug("workingDir = %s" % self.workingDir)
        
        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:
            self.jobInputFiles.append(self.specFiles[self.mainJobSpecName])
            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])
            self.singleSpecName = \
                 self.singleSpecName[:self.singleSpecName.find('-JobSpec.xml')]
            logging.debug("singleSpecName \"%s\"" % self.singleSpecName)
            try :
                jobs = self.bossLiteSession.loadJobByName(
                    self.singleSpecName
                    )

                # handle failures
                if jobs is None :
                    raise JSException("no jobs matching in the BossLite DB", \
                                      FailureList = self.toSubmit.keys())

                if len( jobs ) != 1 :
                    raise JSException("Too many intances in the BossLite DB", \
                                      FailureList = self.toSubmit.keys())

                # taking the job
                self.bossJob = jobs[0]
                logging.info("resubmitting \"%s\"" % self.bossJob['name'])
            except JobError, ex:
                raise JSException(str(ex), FailureList = self.toSubmit.keys()) 
        else :
            try :
                self.bossTask = self.bossLiteSession.loadTaskByName(
                    self.mainJobSpecName
                    )
            except TaskError, ex:
                # non instance in db: create it
                pass
        
        #  //
        # // If already declared (i.e. resubmission), just submit 
        #//
        logging.debug("mainJobSpecName = \"%s\"" % self.mainJobSpecName)
        if self.bossJob is not None:
            logging.debug( "BossLiteBulkInterface.doSubmit bossJobId = %s.%s" \
                           % (self.bossJob['taskId'], self.bossJob['jobId']) )
            self.doBOSSSubmit()
            return

        #generate unique wrapper script
        executable = self.mainJobSpecName + '-submit'
        executablePath = "%s/%s" % (self.workingDir, executable) 
        logging.debug("makeWrapperScript = %s" % executablePath)
        self.makeWrapperScript( executablePath, "$1" )

        inpSandbox = ','.join( self.jobInputFiles )
        logging.debug("Declaring to BOSS")
   
        wrapperName = "%s/%s" % (self.workingDir, self.mainJobSpecName)
        try :

            self.bossTask = Task()
            self.bossTask['name'] = self.mainJobSpecName
            self.bossTask['globalSandbox'] = executablePath + ',' + inpSandbox

            for jobSpecId, jobCacheDir in self.toSubmit.items():
                if len(jobSpecId) == 0 :#or jobSpecId in jobSpecUsedList :
                    continue
                job = Job()
                job['name'] = jobSpecId
                job['arguments'] =  jobSpecId
                job['standardOutput'] =  jobSpecId + '.log'
                job['standardError']  =  jobSpecId + '.log'
                job['executable']     =  executable
                job['outputFiles'] = [ jobSpecId + '.log', 
                                       jobSpecId + '.tgz', \
                                       'FrameworkJobReport.xml' ]
                self.bossLiteSession.getNewRunningInstance( job )
                job.runningJob['outputDirectory'] = jobCacheDir \
                                                    + '/Submission1'
                self.bossTask.addJob( job )
            self.bossLiteSession.updateDB( self.bossTask )
            logging.info( "Successfully Created task %s with %d jobs" % \
                          ( self.bossTask['id'], len(self.bossTask.jobs) ) )
            
        except ProdAgentException, ex:
            raise JSException(str(ex), FailureList = self.toSubmit.keys())

        self.doBOSSSubmit()

        return


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
   available_specs="`/bin/ls ./BulkSpecs`"
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

    def makeWrapperScript(self, filename, jobName):
        """
        _makeWrapperScript_

        Make the main executable script for condor to execute the
        job
        
        """
        
        #  //
        # // Generate main executable script for job
        #//
        script = ["#!/bin/sh\n\n"]
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("JOB_SPEC_NAME=%s\n" % jobName)
        if self.isBulk:
            script.append("BULK_SPEC_NAME=\"%s\"\n" % self.specSandboxName )
            script.append( self.bulkUnpackerScript )
#            script.extend(bulkUnpackerScript(self.specSandboxName))
        else:
#AF            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
            script.append(
                "JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s-JobSpec.xml\n" \
                          % self.singleSpecName
                )

            
        script.append(
            "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % self.mainSandboxName 
            )
        script.append("cd %s\n" % self.workflowName)
        script.append( "./run.sh $JOB_SPEC_FILE > %s.out 2> %s.err\n" \
                       % ( jobName, jobName ) )

        script.append( "tar cvzf $PRODAGENT_JOB_INITIALDIR/%s.tgz  %s.out %s.err\n" \
                       % ( jobName, jobName, jobName ) )

        # Handle missing FrameworkJobReport
#        script.extend(missingJobReportCheck(jobName))
        script.append(self.missingRepScript)

        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()
        return

    
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)
                             

    def doBOSSSubmit(self):
        """
        _doSubmit_

        Build and run a submit command

        """

        # is there a job? build a task!
        if self.bossTask is None and self.bossJob is not None:
            logging.info( "Loading task for job resubmission..."  )

            # close previous instance and set up the outdir
            if self.bossJob.runnningJob['closed'] == 'Y' :
                outdir = self.bossJob.runningJob['outputDirectory']
                self.bossLiteSession.getNewRunningInstance( self.bossJob )
                outdir = outdir[ : outdir.rfind('/') ] + '/Submission' \
                         + str( self.bossJob.runningJob['submission'] )
                self.bossJob.runningJob['outputDirectory'] = outdir

            # load the task ans append the job
            self.bossTask = \
                          self.bossLiteSession.loadTask(self.bossJob['taskId'], False)
            self.bossTask.appendJob( self.bossJob )

        # still no task? Something bad happened
        if self.bossTask is not None :
            logging.debug( "BossLiteBulkInterface.doSubmit bossTask = %s" \
                           % self.bossTask['id'] )
        else:
            raise JSException("Failed to find Job", \
                              FailureList = self.toSubmit.keys())

        #  // retrieve scheduler additiona info
        # //  and eventually change the RB
        #//   according to user provided configuration files
        schedulerConfig = self.getSchedulerConfig()

        #  // build scheduler sedssion, which also checks proxy validity
        # //  an exception raised will stop the submission
        #//
        try:
            schedulerCladFile = self.getSchedulerConfig()
            schedulerConfig = { 'name' : self.scheduler,
                                'config' : schedulerCladFile }
            schedSession = BossLiteAPISched( self.bossLiteSession, \
                                             schedulerConfig )

        except SchedulerError, err:
            raise JSException( "Unable to find a valid certificate", \
                               FailureList = self.toSubmit.keys() ) 
 

        
        # // prepare extra jdl attributes
        logging.info("doBOSSSubmit : preparing jdl")
        try:
            jobType = self.primarySpecInstance.parameters['JobType']
            submissionAttrs = self.createSchedulerAttributes(jobType)
        except Exception, ex:
#           raise JSException("Failed to createJDL", mainJobSpecName = self.mainJobSpecName))
            pass

        # do we need to write the file?
        try :
            schedulercladfile = "%s/%s_scheduler.clad" % \
                                (self.workingDir , self.mainJobSpecName )
            declareClad = open(schedulercladfile,"w")
            declareClad.write( schedSession.jobDescription( self.bossTask, \
                                                    requirements=submissionAttrs ) )
            declareClad.close()
        except:
            pass

        # // Executing BOSS Submit command
        try :
            logging.debug ("BossLiteBulkInterface.doSubmit" )
            self.bossTask = schedSession.submit( self.bossTask, \
                                                 requirements=submissionAttrs )
        except SchedulerError, err:
            logging.error( "########### Failed submission : %s" %str(err) )


        # check for not submitted jobs
        for job in self.bossTask.jobs :
            if job.runningJob['schedulerId'] is None:
                self.failedSubmission.append( job['name'] )

        #  //
        # // Raise Submission Failed
        #//
        if self.failedSubmission != []:
            raise JSException("Submission Failed", \
                              FailureList = self.failedSubmission)

        return


    
    def publishSubmitToDashboard( self ):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        """

        if  self.usingDashboard['use'] != 'True':
            return
        
        dashboardInfo = DashboardInfo()
        appData = str(self.applicationVersions)
        appData = appData.replace("[", "")
        appData = appData.replace("]", "")
        whitelist = str(self.whitelist)
        whitelist = whitelist.replace("[", "")
        whitelist = whitelist.replace("]", "")

        for job in self.bossTask.jobs :
            if job.runningJob['schedulerId'] is None:
                continue

            # compose DashboardInfo.xml path
            outdir = job.runningJob['outputDirectory']
            outdir = outdir[ : outdir.rfind('/') ]
            dashboardInfoFile = os.path.join(outdir, "DashboardInfo.xml" )

            if  not os.path.exists(dashboardInfoFile):
                logging.error("Unable to find dashboardInfoFile " + \
                              dashboardInfoFile )
                continue

            try:
                # it exists, get dashboard information
                dashboardInfo.read(dashboardInfoFile)

            except StandardError, msg:
                # it does not work, abandon
                logging.error("Reading dashboardInfoFile " + \
                              dashboardInfoFile + " failed (jobId=" \
                              + str(job['jobId']) + ")\n" + str(msg))
                continue
        
            # assign job dashboard id
            if dashboardInfo.task == '' :
                logging.error( "unable to retrieve DashboardId for job " + \
                               job['name'] )
                continue
        
            # job basic information
            dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
            dashboardInfo['Scheduler'] = self.__class__.__name__
            dashboardInfo['GridJobID'] = job.runningJob['schedulerId']
            dashboardInfo['SubTimeStamp'] = job.runningJob['submissionTime']
            dashboardInfo['ApplicationVersion'] = appData
            dashboardInfo['TargetCE'] = whitelist
            
            dashboardInfo.write( dashboardInfoFile )
            logging.info("Created dashboardInfoFile " + dashboardInfoFile )

            # publish to Dashboard
            logging.debug("dashboardinfo: %s" % dashboardInfo.__str__())
            dashboardInfo.addDestination(
                self.usingDashboard['address'], self.usingDashboard['port']
                )
            dashboardInfo.publish(5)
        return


    def createSchedulerAttributes(self, jobType):
        """
        _createSchedulerAttributes_
    
        create the scheduler attributes
        """

        return ''


    def getSchedulerConfig(self) :
        """
        _getSchedulerConfig_

        retrieve configuration info for the BossLite scheduler
        """

        return ''


