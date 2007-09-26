#!/usr/bin/env python
"""
_GLITESubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: GLITESubmitter.py,v 1.6 2007/07/02 21:23:29 fvlingen Exp $"

#  //
# // Configuration variables for this submitter
#//
#bossJobType = ""  # some predetermined type value from boss install here
bossScheduler = "glite"

#  //
# // End of Config variables.
#//
import time
import os
import sys
import logging
import exceptions
from ProdCommon.MCPayloads.JobSpec import JobSpec
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.Configuration import ProdAgentConfiguration

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentBOSS import BOSSCommands

from popen2 import Popen4
import select
import fcntl
import string
class InvalidFile(exceptions.Exception):
  def __init__(self,msg):
   args="%s\n"%msg
   exceptions.Exception.__init__(self, args)
   pass


class GLITESubmitter(SubmitterInterface):
    """
    _GLITESubmitter_

    Simple BOSS Submission wrapper for testing.

    """
    
    def __init__(self):
        SubmitterInterface.__init__(self)
        #  //
        # // BOSS installation consistency check.
        #//
##        if not os.environ.has_key("BOSSDIR"):
##            msg = "Error: BOSS environment BOSSDIR not set:\n"
##            raise RuntimeError, msg





        self.bossJobId=0
        self.parameters['Scheduler']="glite"

    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)

        if not self.pluginConfig.has_key("GLITE"):
            msg = "Submitter Plugin Config contains no GLITE Config:\n"
            msg += self.__class__.__name__
            logging.error(msg)
            raise JSException(msg, ClassInstance = self)

        logging.debug(" plugin configurator %s"%self.pluginConfig)


    #  //
    # //  Initially start with the default wrapper script
    #//   provided by the SubmitterInterface base class
    #  //
    # //  If this needs to be customised, implement the 
    #//   generateWrapper method
    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        override default wrapper to generate stdout file

        """
        script = ["#!/bin/sh\n"]
        script.append("tar -zxf %s\n" % os.path.basename(tarballName))
        script.append("cd %s\n" % jobname)
        script.append("./run.sh \n")
        script.append("cd ..\n")
        script.append("cp %s/FrameworkJobReport.xml . \n" % jobname)
##         script.append("cp %s/*/*.root .\n" % jobname )

        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()

        return 
        
        
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_


        Override Submission action to construct a BOSS submit command
        and run it

        Initial tests: No FrameworkJobReport yet, stage back stdout log
        
        """
        self.bossJobId=BOSSCommands.isBOSSDeclared(self.parameters['Wrapper'],self.parameters['JobName'])
        if self.bossJobId==None:
            BOSSCommands.declareToBOSS(self.bossCfgDir,self.parameters)
            self.bossJobId=BOSSCommands.isBOSSDeclared(self.parameters['Wrapper'],self.parameters['JobName'])
        #bossJobId=self.getIdFromFile(TarballDir, JobName)
        logging.debug( "GLITESubmitter.doSubmit bossJobId = %s"%self.bossJobId)
        if self.bossJobId==0:
            raise ProdAgentException("Failed Job Declaration")
        JobName=self.parameters['JobName']
        swversion=self.parameters['AppVersions'][0]  # only one sw version for now


        ## prepare scheduler related file 
        schedulercladfile = "%s/%s_scheduler.clad" % (os.path.dirname(self.parameters['Wrapper']),self.parameters['JobName'])
        try:
           #self.createJDL(schedulercladfile,swversion)
           jobType=self.parameters['JobSpecInstance'].parameters['JobType']
 	   userJDL=self.getUserJDL(jobType)
           self.createJDL(schedulercladfile,userJDL)

        except InvalidFile, ex:
          raise ProdAgentException("Failed to create JDL: %s"%ex)

          
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
            # raise ProdAgentException("Proxy Expired")
          # sys.exit()
            
        bossSubmit = BOSSCommands.submit(self.bossJobId,self.parameters['Scheduler'],self.bossCfgDir)
        try:

          if self.parameters['RTMon']!='':
            bossSubmit+="-rtmon %s "%self.parameters['RTMon']
        except:
          pass
        bossSubmit += " -schclassad %s"%schedulercladfile     #  //
        # // Executing BOSS Submit command
        #//
        # AF : remove the following buggy logging
        #logging.debug( "GLITESubmitter.doSubmit:", bossSubmit)
        output = BOSSCommands.executeCommand(bossSubmit)
        logging.debug ("GLITESubmitter.doSubmit: %s" % output)
        if output.find("error")>=0:
          BOSSCommands.FailedSubmission(str(bossJobId),self.bossCfgDir)
          raise ProdAgentException("Submission Failed")
        #os.remove(cladfile)

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
        #self.editDashboardInfo(self.parameters['DashboardInfo'])


        
        #self.editMyDashboardInfo(self.parameters['DashboardInfo'],bossJobId)
        
        return


    def getUserJDL(self,jobType):
        """
        _getUserJDL_
                                                                                        
        get the user defined JDL in the Submitter config file according to the job type:          o Merge type: look for MergeJDLRequirementsFile first, then default to JDLRequirementsFile
          o Porcessing type: look for JDLRequirementsFile
        """
        UserJDLRequirementsFile="None"
        #
        #  For Merge jobs use Merge JDLRequirementsFile if it's configured
        #
        if jobType == "Merge":
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

    def createJDL(self, cladfilename,UserJDLRequirementsFile):
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
                    user_requirements=" %s && "%UserReq
          else:
            msg="JDLRequirementsFile File Not Found: %s"%UserJDLRequirementsFile
            logging.error(msg)
            raise InvalidFile(msg)
                                                                                                                 
        #  //
        # // white list for anymatch clause
        #//
        anyMatchrequirements=""

        logging.debug("Whitelist is empty %s"%(self.parameters['Whitelist']!=[]))
        if self.parameters['Whitelist']!=[]:
          anyMatchrequirements=" && ("
          sitelist=""
          for i in self.parameters['Whitelist']:
            logging.debug("Whitelist element %s"%i)
            sitelist+="Member(\"%s\",other.GlueCESEBindGroupSEUniqueID)"%i+" || "
          sitelist=sitelist[:len(sitelist)-4]
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
          archrequirement=" && Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) "%swarch
        else:
          archrequirement=""
                                                                                                                 
        #  //
        # // software version requirements
        #//
        #  if len(self.parameters['AppVersions'])> 0:
        #     swVersion=self.parameters['AppVersions'][0]  #
        swClause = "("
        for swVersion in self.applicationVersions:
            swClause += "Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) " % swVersion            if swVersion != self.applicationVersions[-1]:
                # Not last element, need logical AND
                swClause += " && "
        swClause += ")"

        #  //
        # // building jdl
        #//
        requirements = 'Requirements = %s %s %s %s ;\n' \
                       %(user_requirements,swClause,archrequirement,anyMatchrequirements)

        logging.debug('%s'%requirements)
        declareClad.write(requirements)
        declareClad.write("Environment = {\"PRODAGENT_DASHBOARD_ID=%s\"};\n"%self.parameters['DashboardID'])         
        declareClad.write("VirtualOrganisation = \"cms\";\n")

        ## change the RB according to user provided RB configuration files
        if not 'WMSconfig' in self.pluginConfig['GLITE'].keys():
           self.pluginConfig['GLITE']['WMSconfig']=None
        if self.pluginConfig['GLITE']['WMSconfig']!=None and self.pluginConfig['GLITE']['WMSconfig']!='None':
           if not os.path.exists(self.pluginConfig['GLITE']['WMSconfig']) :
              msg="WMSconfig File Not Found: %s"%self.pluginConfig['GLITE']['WMSconfig']
              logging.error(msg)
              raise InvalidFile(msg)
           declareClad.write('WMSconfig = "'+self.pluginConfig['GLITE']['WMSconfig']+'";\n')


        declareClad.close()
        return

    def publishSubmitToDashboard(self, dashboardInfo):
      return


    def editDashboardInfo(self, dashboardInfo):
        """
        _editDashboardInfo_
        
        Add data about submission to DashboardInfo dictionary before
        it is published to the dashboard
        
        
        If dashboardInfo is None, it is not available for this job.

        """

        taskid=(self.bossJobId).split(".")[0]
        chainid=(self.bossJobId).split(".")[1]
        resub=(self.bossJobId).split(".")[2]
        jobGridId=BOSSCommands.schedulerId(self.bossJobId,self.bossCfgDir)
        rbName=(jobGridId.split("/")[2]).split(":")[0]
        logging.info("Scheduler id from GLITESubmitter=%s"%jobGridId)
        logging.info("RB name from GLITESubmitter=%s"%rbName)
        dashboardInfo['ApplicationVersion'] = self.listToString(self.parameters['AppVersions'])
        dashboardInfo['TargetCE'] = self.listToString(self.parameters['Whitelist'])
        dashboardInfo['JSToolUI']= os.environ['HOSTNAME']
        # dashboardInfo.job=dashboardInfo.job+"_"+jobGridId
        dashboardInfo['Scheduler']='gLite'
        dashboardInfo['GridJobID']=jobGridId
        dashboardInfo['RBname']=rbName
        #dashboardInfo.destinations={}
        logging.info("DashboardInfo.job=%s"%dashboardInfo.job)

        #dashboardInfo.write(os.path.join(os.path.dirname(self.parameters['JobCacheArea']), 'DashboardInfo.xml'))
        dashboardinfodir=BOSSCommands.subdir(self.bossJobId,self.bossCfgDir)
        #dashboardInfo.write(os.path.join(os.path.dirname(self.parameters['JobCacheArea']) , 'DashboardInfo.xml'))
        try:
          dashboardCfg = self.pluginConfig.get('Dashboard', {})
          usingDashboard = dashboardCfg.get("UseDashboard", "False")
          DashboardAddress = dashboardCfg.get("DestinationHost")
          DashboardPort=dashboardCfg.get("DestinationPort")
          dashboardInfo.addDestination(DashboardAddress, int(DashboardPort))

          logging.debug("DashboardInfo=%s"%dashboardInfo.__str__())
        except:
          usingDashboard="False"
          logging.info("No Dashboard section in SubmitterPluginConfig")

        logging.debug("usingdashboard.lower()=='true' =%s"%(usingDashboard.lower()=='true'))
        
        if  usingDashboard.lower()=='true':
          dashboardInfo.publish(5)
#          dashboardInfo.clear()  
          dashboardInfo.write(dashboardinfodir + "/DashboardInfo%s_%s_%s.xml"%(taskid,chainid,resub))

        return


      
registerSubmitter(GLITESubmitter, GLITESubmitter.__name__)
