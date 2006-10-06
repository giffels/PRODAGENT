#!/usr/bin/env python
"""
_LCGSubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: LCGSubmitter.py,v 1.18 2006/09/28 13:58:25 bacchi Exp $"

#  //
# // Configuration variables for this submitter
#//
#bossJobType = ""  # some predetermined type value from boss install here
bossScheduler = "edg"

#  //
# // End of Config variables.
#//
import time
import os
import sys
import logging
import exceptions
from MCPayloads.JobSpec import JobSpec
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException
from ProdAgentCore.ProdAgentException import ProdAgentException
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


class LCGSubmitter(SubmitterInterface):
    """
    _LCGSubmitter_

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



        self.bossStrJobId=""

        
        self.parameters['Scheduler']="edg"


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)

        if not self.pluginConfig.has_key("LCG"):
            msg = "Submitter Plugin Config contains no LCG Config:\n"
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
        bossJobId=self.isBOSSDeclared()
        if bossJobId==None:
            self.declareToBOSS()
            bossJobId=self.isBOSSDeclared()
        #bossJobId=self.getIdFromFile(TarballDir, JobName)
        logging.debug( "LCGSubmitter.doSubmit bossJobId = %s"%bossJobId)
        if bossJobId==0:
            raise ProdAgentException("Failed Job Declaration")
        JobName=self.parameters['JobName']
        swversion=self.parameters['AppVersions'][0]  # only one sw version for now


        ## prepare scheduler related file 
        schedulercladfile = "%s/%s_scheduler.clad" % (os.path.dirname(self.parameters['Wrapper']),self.parameters['JobName'])
        try:
           self.createJDL(schedulercladfile,swversion)
        except InvalidFile, ex:
           return 

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
            
        bossSubmit = BOSSCommands.submit(bossJobId,self.parameters['Scheduler'],self.bossCfgDir)
        try:

          if self.parameters['RTMon']!='':
            bossSubmit+="-rtmon %s "%self.parameters['RTMon']
        except:
          pass
        bossSubmit += " -schclassad %s"%schedulercladfile     #  //
        # // Executing BOSS Submit command
        #//
        # AF : remove the following buggy logging
        #logging.debug( "LCGSubmitter.doSubmit:", bossSubmit)
        output = BOSSCommands.executeCommand(bossSubmit)
        logging.debug ("LCGSubmitter.doSubmit: %s" % output)
        if output.find("error")>=0:
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

        self.bossStrJobId=str(bossJobId)+"."+chainid+"."+resub
        logging.info("Submitter bossJobId=%s"%bossJobId)
        #self.editDashboardInfo(self.parameters['DashboardInfo'])
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
        try:
          taskid=self.bossStrJobId.split(".")[0]
          chainid=self.bossStrJobId.split(".")[1]
          resub=self.bossStrJobId.split(".")[2]
        except:
          return
        jobGridId=BOSSCommands.schedulerId(self.bossStrJobId,self.bossCfgDir)
        rbName=(jobGridId.split("/")[2]).split(":")[0]
        #logging.info("Scheduler id from LCGSubmitter=%s"%jobGridId)
        dashboardInfo['ApplicationVersion'] = self.listToString(self.parameters['AppVersions'])
        dashboardInfo['TargetCE'] = self.listToString(self.parameters['Whitelist'])
        dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
         
        dashboardInfo.job=dashboardInfo.job+"_"+jobGridId
        dashboardInfo['Scheduler']='LCG'
        dashboardInfo['GridJobID']=jobGridId
        dashboardInfo['RBname']=rbName
#        dashboardInfo.destinations={}
        dashboardinfodir=BOSSCommands.subdir(self.bossStrJobId,self.bossCfgDir)
        #dashboardInfo.write(os.path.join(os.path.dirname(self.parameters['JobCacheArea']) , 'DashboardInfo.xml'))

        try:
          dashboardCfg = self.pluginConfig.get('Dashboard', {})
          usingDashboard = dashboardCfg.get("UseDashboard", "False")
          DashboardAddress = dashboardCfg.get("DestinationHost")
          DashboardPort=dashboardCfg.get("DestinationPort")
          dashboardInfo.addDestination(DashboardAddress, int(DashboardPort))
          logging.debug("DashboardInfo=%s"%dashboardInfo.__str__())
        except:
          logging.info("No Dashboard section in SubmitterPluginConfig")
          usingDashboard="False"
        if  usingDashboard.lower()=='true':
          dashboardInfo.publish(5)
#          dashboardInfo.clear()
          dashboardInfo.write(dashboardinfodir +"/DashboardInfo%s_%s_%s.xml"%(taskid,chainid,resub))

        return
      
    def createJDL(self, cladfilename,swversion):
        """
        _createJDL_
    
        create the scheduler JDL combining the user specified bit of the JDL
        """

        declareClad=open(cladfilename,"w")
                                                                                            
        if not 'JDLRequirementsFile' in self.pluginConfig['LCG'].keys():
          self.pluginConfig['LCG']['JDLRequirementsFile']=None

        ## combine with the JDL provided by the user
        user_requirements=""

        if self.pluginConfig['LCG']['JDLRequirementsFile']!=None and self.pluginConfig['LCG']['JDLRequirementsFile']!='None':
          if os.path.exists(self.pluginConfig['LCG']['JDLRequirementsFile']) :
            UserReq = None
            logging.debug("createJDL: using JDLRequirementsFile "+self.pluginConfig['LCG']['JDLRequirementsFile'])
            fileuserjdl=open(self.pluginConfig['LCG']['JDLRequirementsFile'],'r')
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
            msg="JDLRequirementsFile File Not Found: %s"%self.pluginConfig['LCG']['JDLRequirementsFile']
            logging.error(msg) 
            raise InvalidFile(msg)
        anyMatchrequirements=""
        if self.parameters['Whitelist']!=[]:
          anyMatchrequirements=" && anyMatch(other.storage.CloseSEs , ("
          sitelist=""
          for i in self.parameters['Whitelist']:
            logging.debug("Whitelist element %s"%i)
            sitelist+="target.GlueSEUniqueID==\"%s\""%i+" || "
          sitelist=sitelist[:len(sitelist)-4]
          anyMatchrequirements+=sitelist+"))"
          

        requirements='Requirements = %s Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) %s;\n'%(user_requirements,swversion,anyMatchrequirements)
        logging.debug('%s'%requirements)
        declareClad.write(requirements)
          
        declareClad.write("VirtualOrganisation = \"cms\";\n")

        ## change the RB according to user provided RB configuration files
        if not 'RBconfig' in self.pluginConfig['LCG'].keys():
           self.pluginConfig['LCG']['RBconfig']=None
        if self.pluginConfig['LCG']['RBconfig']!=None and self.pluginConfig['LCG']['RBconfig']!='None':
           if not os.path.exists(self.pluginConfig['LCG']['RBconfig']) :
              msg="RBconfig File Not Found: %s"%self.pluginConfig['LCG']['RBconfig']
              logging.error(msg)
              raise InvalidFile(msg)
           declareClad.write('RBconfig = "'+self.pluginConfig['LCG']['RBconfig']+'";\n')

        if not 'RBconfigVO' in self.pluginConfig['LCG'].keys():
           self.pluginConfig['LCG']['RBconfigVO']=None
        if self.pluginConfig['LCG']['RBconfigVO']!=None and self.pluginConfig['LCG']['RBconfigVO']!='None':
           if not os.path.exists(self.pluginConfig['LCG']['RBconfigVO']) :
              msg="RBconfigVO File Not Found: %s"%self.pluginConfig['LCG']['RBconfigVO']
              logging.error(msg)
              raise InvalidFile(msg)
           declareClad.write('RBconfigVO = "'+self.pluginConfig['LCG']['RBconfigVO']+'";\n')

        declareClad.close()
        return


registerSubmitter(LCGSubmitter, LCGSubmitter.__name__)


