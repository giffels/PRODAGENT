#!/usr/bin/env _GLITEBulkSubmitter_
"""
python

Glite Collection implementation.

"""

__revision__ = "$Id: GLiteBulkSubmitter.py,v 1.2 2007/02/16 18:47:32 evansde Exp $"

import os
import logging


from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from JobSubmitter.Submitters.OSGUtils import standardScriptHeader
from JobSubmitter.Submitters.OSGUtils import bulkUnpackerScript
from JobSubmitter.Submitters.OSGUtils import missingJobReportCheck

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentBOSS import BOSSCommands
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

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

    Globus Universe condor submitter. Generates a simple JDL file
    and condor_submits it using a dag wrapper and post script to generate
    a callback to the ProdAgent when the job completes.
    

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
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.specSandboxName = None
        self.singleSpecName = None
        self.bossJobId = ""
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
            self.bossJobId = BOSSCommands.getIdFromJobName(
                self.bossCfgDir,self.singleSpecName
                )
            logging.debug("resubmitting \"%s\"" % self.bossJobId)
        else :
#            logging.debug("mainJobSpecName = %s" % self.mainJobSpecName)
            self.bossJobId = BOSSCommands.getIdFromJobName(
                self.bossCfgDir, self.mainJobSpecName
                )
            self.bossJobId = self.bossJobId.split('.')[0]
        
        #  //
        # // If already declared (i.e. resubmission), just submit 
        #//
        logging.debug("mainJobSpecName = \"%s\"" % self.mainJobSpecName)
        if self.bossJobId != None and len(self.bossJobId) > 0:
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
            self.doBOSSSubmit()
            return
  
        executable = "%s/%s-submit" % (self.workingDir, self.workflowName)
        logging.debug("makeWrapperScript = %s" % executable)
        #generate unique wrapper script
        self.makeWrapperScript( executable, "$1" )

        inpFileJDL = ""
        for f in self.jobInputFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        logging.debug("Declaring to BOSS")
        self.bossJobId=BOSSCommands.declareBulk(
            self.bossCfgDir, self.toSubmit, inpFileJDL, self.workingDir , self.workflowName
          )

        if self.bossJobId != None and self.bossJobId != "":
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
        else:
            raise ProdAgentException("Failed Job Declaration")

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
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.singleSpecName)   
            
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
                             

#         # expect globus scheduler in OSG block
#         # self.pluginConfig['OSG']['GlobusScheduler']
#         if not self.pluginConfig.has_key("OSG"):
#             msg = "Plugin Config for: %s \n" % self.__class__.__name__
#             msg += "Does not contain an OSG config block"
#             raise JSException( msg , ClassInstance = self,
#                                PluginConfig = self.pluginConfig)

#         #  //
#         # // Validate the value of the GlobusScheduler is present
#         #//  and sane
#         sched = self.pluginConfig['OSG'].get("GlobusScheduler", None)
#         if sched in (None, "None", "none", ""):
#             msg = "Invalid value for OSG GlobusScheduler in Submitter "
#             msg += "plugin configuration: %s\n" % sched
#             msg += "You must provide a valid scheduler"
#             raise JSException( msg , ClassInstance = self,
#                                PluginConfig = self.pluginConfig)
          
#         #  //
#         # // Extract the mapping of SEName to Jobmanager from the
#         #//  plugin config
#         siteMapping = self.pluginConfig.get('SENameToJobmanager', None)
#         if siteMapping == None:
#             msg = "SENameToJobManager not provided in Submitter "
#             msg += "pluging configuration\n"
#             msg += "This is required for mapping merge jobs to the appropriate"
#             msg += "Site based on fileblock name"
#             raise JSException(msg, 
#                               ClassInstance = self,
#                               PluginConfig = self.pluginConfig)

    def doBOSSSubmit(self):
        """
        _doSubmit_

        Build and run a submit command

        """
        
        if self.bossJobId != None and self.bossJobId != "":
            logging.debug( "GLITEBulkSubmitter.doSubmit bossJobId = %s" \
                           %self.bossJobId)
        else:
            raise ProdAgentException("Failed to find Job")

        # proxy check
        logging.info("doBOSSSubmit : proxy check")
        try:
            output=BOSSCommands.executeCommand("voms-proxy-info")
            output=output.split("timeleft")[1].strip()
            output=output.split(":")[1].strip()
            if output=="0:00:00":
                #logging.info( "You need a voms-proxy-init -voms cms")
                logging.error("voms-proxy-init expired")
                #sys.exit()
        except StandardError,ex:
            #print "You need a voms-proxy-init -voms cms"
            logging.error("voms-proxy-init does not exist")
            logging.error(output)
            sys.exit()
            
        logging.info("doBOSSSubmit : preparing jdl")
        ## prepare scheduler related file 
        schedulercladfile = "%s/%s_scheduler.clad" % (self.workingDir ,self.workflowName)
        try:
           self.createJDL(schedulercladfile)
        except InvalidFile, ex:
#           raise ProdAgentException("Failed to createJDL")
           pass



        logging.info("doBOSSSubmit : submit")

        scheduler  = "gliteCollection"
        try:
            scheduler = self.pluginConfig['GLITE']['Scheduler']
            logging.info("BOSS Scheduler: " + scheduler)
        except:
            logging.info("Missing Scheduler, using gliteCollection")
            pass
        
        bossSubmit = BOSSCommands.submit(
            self.bossJobId, scheduler,self.bossCfgDir
            )

        try:
            if self.pluginConfig['GLITE']['RTMon']!='':
                bossSubmit+=" -rtmon %s "%self.pluginConfig['GLITE']['RTMon']
                logging.info("BOSS RTMon: " + self.pluginConfig['GLITE']['RTMon'])
            else:
                bossSubmit+=" -rtmon NONE "
                logging.info("BOSS RTMon not set")
        except:
            bossSubmit+=" -rtmon NONE "
            logging.info("BOSS RTMon not set xx")

        bossSubmit += " -schclassad %s"%schedulercladfile
        #  //
        # // Executing BOSS Submit command
        #//
        output = BOSSCommands.executeCommand(bossSubmit)
        logging.debug ("GLITEBulkSubmitter.doSubmit: %s" % output)
        if output.find("error")>=0:
            # // cleaning if bulk submission failed for max retries
            if self.isBulk:
                BOSSCommands.FailedSubmission (
                    self.bossJobId + '.1',self.bossCfgDir
                    )
            raise ProdAgentException("Submission Failed")

        try:
            resub=output.split("Resubmission number")[1].split("\n")[0].strip()
            logging.debug("resub =%s"%resub)
        except:
            resub="1"
        try:
            chainid=(output.split("Scheduler ID for job")[1]).split("is")[0].strip()
        except:
            raise ProdAgentException("Submission Failed")

        self.bossJobId=str(self.bossJobId)+"."+chainid+"."+resub
        return 




    def createJDL(self, cladfilename):
        """
        _createJDL_
    
        create the scheduler JDL combining the user specified bit of the JDL
        """

        declareClad=open(cladfilename,"w")
                                                                                            
        if not 'JDLRequirementsFile' in self.pluginConfig['GLITE'].keys():
            self.pluginConfig['GLITE']['JDLRequirementsFile']=None

        #  //
        # // combine with the JDL provided by the user
        #//
        user_requirements=""

        if self.pluginConfig['GLITE']['JDLRequirementsFile']!=None and self.pluginConfig['GLITE']['JDLRequirementsFile']!='None':
            if os.path.exists(self.pluginConfig['GLITE']['JDLRequirementsFile']) :
                UserReq = None
                logging.debug(
                    "createJDL: using JDLRequirementsFile " \
                    +self.pluginConfig['GLITE']['JDLRequirementsFile']
                    )
                fileuserjdl = open(
                    self.pluginConfig['GLITE']['JDLRequirementsFile'], 'r'
                    )
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
                    user_requirements=" %s && "%UserReq
            else:
                msg="JDLRequirementsFile File Not Found: %s" \
                     %self.pluginConfig['GLITE']['JDLRequirementsFile']
                logging.error(msg) 
                raise InvalidFile(msg)

        #  //
        # // white list for anymatch clause
        #//
        anyMatchrequirements = ""
        if not 'Whitelist' in self.pluginConfig['GLITE'].keys():
            self.pluginConfig['GLITE']['Whitelist'] = None
        
        if self.pluginConfig['GLITE']['Whitelist']!=None and self.pluginConfig['GLITE']['Whitelist']!='None':
            Whitelist = self.pluginConfig['GLITE']['Whitelist'].split(',')
            anyMatchrequirements = " && ("
            sitelist = ""
            for i in Whitelist:
                logging.debug("Whitelist element %s"%i)
                sitelist+="target.GlueSEUniqueID==\"%s\""%i+" || "
            sitelist = sitelist[:len(sitelist)-4]
            anyMatchrequirements+=sitelist+")"

        #  //
        # // software version requirements
        #//
        swClause = "("
        for swVersion in self.applicationVersions:
            swClause += "Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) " % swVersion
            if swVersion != self.applicationVersions[-1]:
                # Not last element, need logical AND
                swClause += " && "
        swClause += ")"


        #  //
        # // building jdl
        #//
        requirements = 'Requirements = %s %s;%s\n' \
                       %(user_requirements,swClause,anyMatchrequirements)
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
                
            

registerSubmitter(GLiteBulkSubmitter, GLiteBulkSubmitter.__name__)