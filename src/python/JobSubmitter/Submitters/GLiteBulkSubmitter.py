#!/usr/bin/env _GLITEBulkSubmitter_
"""
python

Glite Collection implementation.

"""

__revision__ = "$Id: GLiteBulkSubmitter.py,v 1.19 2008/02/15 12:28:13 afanfani Exp $"
__version__ = "$Revision: 1.19 $"

import os, time, string
import logging


from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.PluginConfiguration import loadPluginConfig

from ProdAgentBOSS import BOSSCommands

from ProdAgentCore.ProdAgentException import ProdAgentException

import exceptions
class InvalidFile(exceptions.Exception):
    def __init__(self,msg):
        args="%s\n"%msg
        exceptions.Exception.__init__(self, args)
        pass

class GLiteBulkSubmitter(BulkSubmitterInterface):
    """
    _GLiteBulkSubmitter_

    GLite Bulk Submitter. May use collection or parametric jobs,
    as selected in the SubmitterPluginConfig.
    Actual submission is made through BOSS.

    """

    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """
        logging.debug("<<<<<<<<<<<<<<<<<GLiteBulkSubmitter>>>>>>>>>>>>>>..")
#        logging.debug("%s" % self.primarySpecInstance.parameters)

        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        if not self.primarySpecInstance.parameters.has_key('BulkInputSandbox'):
            msg="There is no BulkInputSandbox defined in the JobSpec. Submission cant go on..."
            logging.error(msg)
            return
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.specSandboxName = None
        self.singleSpecName = None
        self.bossJobId = ""
        self.submittedJobs = {}
        self.failedSubmission = []
        
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles = []
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
        # // Retrieve BOSS configuration files dir
        #//
        cfgObject = loadProdAgentConfiguration()
        bossConfig = cfgObject.get("BOSS")
        self.bossCfgDir = bossConfig['configDir']
        logging.debug("bossCfgDir = %s" % self.bossCfgDir)

        #  //
        # // check for dashboard usage
        #//
        self.usingDashboard = {'use' : 'True', \
                               'address' : 'lxgate35.cern.ch', \
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
                self.bossJobId = BOSSCommands.getIdFromJobName(
                    self.bossCfgDir,self.singleSpecName
                    )
            except ProdAgentException, ex:
                raise JSException(str(ex), FailureList = self.toSubmit.keys()) 
            logging.debug("resubmitting \"%s\"" % self.bossJobId)
        else :
            try :
                self.bossJobId = BOSSCommands.getIdFromJobName(
                    self.bossCfgDir, self.mainJobSpecName
                    )
                self.bossJobId = self.bossJobId.split('.')[0]
            except ProdAgentException, ex:
                raise JSException(str(ex), FailureList = self.toSubmit.keys())
#                raise JSException(str(ex), mainJobSpecName = self.mainJobSpecName)
        
        #  //
        # // If already declared (i.e. resubmission), just submit 
        #//
        logging.debug("mainJobSpecName = \"%s\"" % self.mainJobSpecName)
        if self.bossJobId != None and len(self.bossJobId) > 0:
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
            self.doBOSSSubmit()
            return
  
#        executable = "%s/%s-submit" % (self.workingDir, self.workflowName)
        executable = "%s/%s-submit" % (self.workingDir, self.mainJobSpecName) 
        logging.debug("makeWrapperScript = %s" % executable)
        #generate unique wrapper script
        self.makeWrapperScript( executable, "$1" )

        inpFileJDL = ""
        for f in self.jobInputFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        logging.debug("Declaring to BOSS")

        
        try :
            self.bossJobId = \
                           BOSSCommands.declareBulk(
                self.bossCfgDir, self.toSubmit, inpFileJDL,
                self.workingDir , self.workflowName, self.mainJobSpecName
                )
        except ProdAgentException, ex:
            raise JSException(str(ex), FailureList = self.toSubmit.keys())
#            raise JSException(str(ex), mainJobSpecName = self.mainJobSpecName)

        if self.bossJobId != None and self.bossJobId != "":
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
        else:
#            raise JSException("Failed Job Declaration", mainJobSpecName = self.mainJobSpecName)
            raise JSException("Failed Job Declaration", FailureList = self.toSubmit.keys()) 

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
        script.append("./run.sh $JOB_SPEC_FILE\n")

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
        
        if self.bossJobId != None and self.bossJobId != "":
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
        else:
#            raise JSException("Failed to find Job", mainJobSpecName = self.mainJobSpecName)
            raise JSException("Failed to find Job", FailureList = self.toSubmit.keys()) 


        # // Check proxy validity: an exception raised will stop the submission
        #//
        try :
            BOSSCommands.checkUserProxy()
        except ProdAgentException:
            raise JSException( "Unable to find a valid certificate", \
                               FailureList = self.toSubmit.keys() ) 
            
        logging.info("doBOSSSubmit : preparing jdl")
        
        ## prepare scheduler related file 
#        schedulercladfile = "%s/%s_scheduler.clad" % (self.workingDir ,self.workflowName)
        schedulercladfile = "%s/%s_scheduler.clad" % (self.workingDir , self.mainJobSpecName )
        try:
            jobType=self.primarySpecInstance.parameters['JobType']
            userJDL=self.getUserJDL(jobType)
            self.createJDL(schedulercladfile,jobType,userJDL)
        except InvalidFile, ex:
#           raise JSException("Failed to createJDL", mainJobSpecName = self.mainJobSpecName))
            pass


        #  //
        # // Executing BOSS Submit command
        #//
        bossSubmit = ""
        # // scheduler
        self.scheduler  = "gliteCollection"
        try:
            self.scheduler = self.pluginConfig['GLITE']['Scheduler']
            logging.info("BOSS Scheduler: " + self.scheduler)
        except:
            logging.info("Missing Scheduler, using gliteCollection")
            pass
        # // RTMon
        try:
            if self.pluginConfig['GLITE']['RTMon']!='':
                bossSubmit+=" -rtmon %s "%self.pluginConfig['GLITE']['RTMon']
                logging.info("BOSS RTMon: " + self.pluginConfig['GLITE']['RTMon'])
            else:
                bossSubmit += " -rtmon NONE "
                logging.info("BOSS RTMon not set")
        except:
            bossSubmit +=  " -rtmon NONE "
            logging.info("BOSS RTMon not set xx")
        # // submission command
        bossSubmit = BOSSCommands.submit(
            self.bossJobId, self.scheduler, self.bossCfgDir
            )
        bossSubmit += " -schclassad %s" % schedulercladfile
        # write submission logs
        bossSubmit += " -logfile %s/%s-%s-submitLog " % (self.toSubmit[self.mainJobSpecName], self.mainJobSpecName , time.time())
        #  //
        # // Executing BOSS Submit command
        #//
        logging.debug ("GLITEBulkSubmitter.doSubmit: %s" % bossSubmit)
        #
        # execute command with timeout based on nb.of jobs = len(self.toSubmit) with 60sec for each job
        #
        output = BOSSCommands.executeCommand(bossSubmit,len(self.toSubmit)*60)

        #  //
        # // Check Submission Failed
        #//
        logging.debug ("GLITEBulkSubmitter.doSubmit: output %s output" % output)
        failurejobs = []
        if output.find("error")>=0 or output.find("failed submission")>=0:
            if self.isBulk:
            # // cleaning if bulk submission failed for max retries
            #    BOSSCommands.FailedSubmission (
            #        self.bossJobId + '.1',self.bossCfgDir
            #        )
            #
                failurejobs = self.checkPartialSubmissionFailure()
            else :
                failurejobs = self.toSubmit.keys()

        #  // retrieve actually submitted jobs with their scheduler ID
        # //  needed by the Dashboard
        taskSubmittedJobs = BOSSCommands.submittedJobs(
            self.bossJobId, self.bossCfgDir
            )
        logging.debug( "########### " + taskSubmittedJobs.__str__() )
        #  // check which jobs are actually submitted, filling up a dictionary
        # //  with submittedJobs<->schedId and a list of failed
        # //  (can be also used to report partial submissions...)
        if not self.isBulk:
            if self.singleSpecName in taskSubmittedJobs.keys() :
                self.submittedJobs[ self.singleSpecName ] = \
                                    taskSubmittedJobs[ self.singleSpecName ]
            else:
                self.failedSubmission.append( self.singleSpecName )
                                                                                                                      
        # in case of bulk, check which jobs have a scheduler ID
        else :
            for jobSpecName in self.toSubmit.keys() :
                if jobSpecName in taskSubmittedJobs.keys() :
                    self.submittedJobs[ jobSpecName ] = \
                                        taskSubmittedJobs[ jobSpecName ]
                else :
                    self.failedSubmission.append( jobSpecName )
        logging.debug( "########### " + self.submittedJobs.__str__() )

        #  //
        # // Raise Submission Failed
        #//
        if len(failurejobs)>0:
            raise JSException("Submission Failed", FailureList = failurejobs)

        # // retrieving submission number
        try:
            resub=output.split("Resubmission number")[1].split("\n")[0].strip()
            logging.debug("resub =%s"%resub)
        except:
            resub="1"

        # // remove cladfile
        os.remove(schedulercladfile)

        return 

    def checkPartialSubmissionFailure(self):
        """
        _checkPartialSubmissionFailure_

         check which jobs failed the submission and return a list of jobnames that failed in submission, 
         so that one can send a Submission Failure only for those
        """
        failurejobs = []
        inJobList=string.join(self.toSubmit.keys(),"','")
        inJobList="( '%s' )" %inJobList
        command = "bossAdmin SQL -query \"select CHAIN.NAME from JOB,CHAIN where CHAIN.TASK_ID=JOB.TASK_ID  and CHAIN.ID=JOB.CHAIN_ID and CHAIN.NAME in %s and JOB.STATUS='W'\" -c %s"%(inJobList, self.bossCfgDir)
        outjobs=BOSSCommands.executeCommand( command )
        for ajob in outjobs.strip().split('\n'):
            job=ajob.strip()
            if job != "NAME" and job != "No results!":
                failurejobs.append(job)
        return failurejobs

    def getUserJDL(self,jobType):
        """
        _getUserJDL_
 
        get the user defined JDL in the Submitter config file according to the job type:
          o Merge type: look for MergeJDLRequirementsFile first, then default to JDLRequirementsFile
          o Porcessing type: look for JDLRequirementsFile 
        """
        UserJDLRequirementsFile="None"
        #
        #  For Merge jobs use Merge JDLRequirementsFile if it's configured
        #
        if jobType == "Merge" or jobType == "CleanUp":
            if 'MergeJDLRequirementsFile' in self.pluginConfig['GLITE'].keys():
                UserJDLRequirementsFile=self.pluginConfig['GLITE']['MergeJDLRequirementsFile']
                return UserJDLRequirementsFile
        #
        #  Use JDLRequirementsFile if it's configured
        #
        if 'JDLRequirementsFile' in self.pluginConfig['GLITE'].keys():
            UserJDLRequirementsFile=self.pluginConfig['GLITE']['JDLRequirementsFile']
            return UserJDLRequirementsFile

        return UserJDLRequirementsFile


    def createJDL(self, cladfilename,jobType,UserJDLRequirementsFile):
        """
        _createJDL_
    
        create the scheduler JDL combining the user specified bit of the JDL
        """

        declareClad=open(cladfilename,"w")

        #  //
        # // combine with the JDL provided by the user
        #//
        user_requirements=""

        if UserJDLRequirementsFile!="None":

          if os.path.exists(UserJDLRequirementsFile) :
            UserReq = None
            logging.debug("createJDL: using JDLRequirementsFile "+UserJDLRequirementsFile)
            fileuserjdl=open(UserJDLRequirementsFile,'r')
            inlines=fileuserjdl.readlines()
            for inline in inlines :
              ## extract the Requirements specified by the user
              if inline.find('Requirements') > -1 and inline.find('#') == -1 :
                UserReq = inline[ inline.find('=')+2 : inline.find(';') ]
              ## write the other user defined JDL lines as they are
              else :
                if inline.find('#') != 0 and len(inline) > 1 :
                   declareClad.write(inline)
            if UserReq != None :
                    user_requirements=" %s "%UserReq
          else:
            msg="JDLRequirementsFile File Not Found: %s"%UserJDLRequirementsFile
            logging.error(msg)
            raise InvalidFile(msg)

        #  //
        # // white list for anymatch clause
        #//
        anyMatchrequirements = ""
        if len(self.whitelist)>0:
            Whitelist = self.whitelist
            anyMatchrequirements = " ("
            sitelist = ""
            for i in Whitelist:
                logging.debug("Whitelist element %s"%i)
                sitelist+="Member(\"%s\" , other.GlueCESEBindGroupSEUniqueID)"%i+" || "
                #sitelist+="target.GlueSEUniqueID==\"%s\""%i+" || "
            sitelist = sitelist[:len(sitelist)-4]
            anyMatchrequirements+=sitelist+")"

        #  //
        # // CMSSW arch
        #//
        swarch = None
        creatorPluginConfig = loadPluginConfig("JobCreator",
                                                  "Creator")
        if creatorPluginConfig['SoftwareSetup'].has_key('ScramArch'):
           if creatorPluginConfig['SoftwareSetup']['ScramArch'].find("slc4")>=0:
              swarch=creatorPluginConfig['SoftwareSetup']['ScramArch']

        if swarch:
          archrequirement = " Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) "%swarch
        else:
          archrequirement = ""

        #  //
        # // software version requirements
        #//
        if jobType=="CleanUp":
            swClause = ""
        else:
            if len(self.applicationVersions)>0:
                swClause = " ("
                for swVersion in self.applicationVersions:
                    swClause += "Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) " % swVersion
                    if swVersion != self.applicationVersions[-1]:
                    # Not last element, need logical AND
                        swClause += " && "
                swClause += ")"
            else:
                raise ProdAgentException("No CMSSW version found!")


        #  //
        # // building jdl
        #//

        requirements = "Requirements = %s " % user_requirements
        if swClause != "":
           requirements += " && %s " % swClause 
        if archrequirement != "" :
           requirements += " && %s " % archrequirement
        if anyMatchrequirements != "" :
           requirements += " && %s " %anyMatchrequirements
        logging.info('%s'%requirements)
        declareClad.write(requirements)
#        declareClad.write("Environment = {\"PRODAGENT_DASHBOARD_ID=%s\"};\n"%self.parameters['DashboardID'])
        declareClad.write("VirtualOrganisation = \"cms\";\n")

        #  //
        # // change the RB according to user provided RB configuration files
        #//
        if not 'WMSconfig' in self.pluginConfig['GLITE'].keys():
            self.pluginConfig['GLITE']['WMSconfig']=None
        if self.pluginConfig['GLITE']['WMSconfig']!=None and self.pluginConfig['GLITE']['WMSconfig']!='None':
            if not os.path.exists(self.pluginConfig['GLITE']['WMSconfig']) :
                msg = "WMSconfig File Not Found: %s" \
                      %self.pluginConfig['GLITE']['WMSconfig']
                logging.error(msg)
                raise InvalidFile(msg)
            declareClad.write(
                'WMSconfig = "'+self.pluginConfig['GLITE']['WMSconfig']+'";\n'
                )

        declareClad.close()
        return


    
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
        

        for jobSpecId, jobSchedId in self.submittedJobs.iteritems() :
            ( dashboardInfo, dashboardInfoFile ) = \
              BOSSCommands.guessDashboardInfo( 
                self.bossJobId, jobSpecId, self.bossCfgDir
                )
        
            # assign job dashboard id
            if dashboardInfo.task == '' :
                logging.error( "unable to retrieve DashboardId for job " + \
                               jobSpecId )
                continue
        
            # job basic information
            dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
            dashboardInfo['Scheduler'] = self.__class__.__name__
            dashboardInfo['GridJobID'] = jobSchedId
            dashboardInfo['SubTimeStamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
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
      



registerSubmitter(GLiteBulkSubmitter, GLiteBulkSubmitter.__name__)
