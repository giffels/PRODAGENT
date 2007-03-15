#!/usr/bin/env python
"""
_RESubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: RESubmitter.py,v 1.3 2007/03/13 11:48:58 bacchi Exp $"

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
from ProdCommon.MCPayloads.JobSpec import JobSpec
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


class RESubmitter(SubmitterInterface):
    """
    _RESubmitter_

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
            # raise JSException( msg, ClassInstance = self)


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
        
    def __call__(self, workingDir, jobCreationArea, jobname, **args):

      self.parameters.update(args)
      logging.info("jobname=%s"%jobname)
      tmp=jobname.split('_')
      chainId=tmp[len(tmp)-1]
      taskName='_'.join(tmp[:len(tmp)-1])
      logging.info("TaskName=%s"%taskName)
      taskId=BOSSCommands.getTaskIdFromName(taskName,self.bossCfgDir)
      logging.info("TaskId=%s"%taskId)
      self.parameters['TaskId']=taskId+"."+chainId
      self.parameters['JobName']='_'.join(tmp[:len(tmp)-1])
      logging.info("taskid=%s"%self.parameters['TaskId'])
      
      self.doSubmit("", "")
      
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_


        Override Submission action to construct a BOSS submit command
        and run it

        Initial tests: No FrameworkJobReport yet, stage back stdout log
        
        """
        logging.info("taskid%s"%self.parameters['TaskId'])

        try:
          taskid=self.parameters['TaskId'].split('.')[0]
          chainid=self.parameters['TaskId'].split('.')[1]
          int(taskid)
          int(chainid)
          bossJobId=self.parameters['TaskId']
        except:
          bossJobId=None
          # bossJobId=self.isBOSSDeclared()
        subDir=BOSSCommands.subdir(bossJobId+".1",self.bossCfgDir)
        logging.info("subDir= %s"%subDir)
        cert=subDir.split('share')[0]+"share/userProxy"
        logging.info("proxy fullpath %s"%cert)
        if os.path.exists(cert):
          os.environ["X509_USER_PROXY"]=cert

        logging.info("bossJobId%s"%bossJobId)
        #bossJobId=self.getIdFromFile(TarballDir, JobName)
        logging.debug( "RESubmitter.doSubmit bossJobId = %s"%bossJobId)
        if bossJobId==None:
            raise ProdAgentException("Failed Job Declaration")


        ## prepare scheduler related file 
        # schedulercladfile = "%s/%s_scheduler.clad" % (os.path.dirname(self.parameters['Wrapper']),self.parameters['JobName'])
        
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
            raise ProdAgentException("Proxy Expired")
            # sys.exit()
            
        bossSubmit = BOSSCommands.resubmit(bossJobId,self.bossCfgDir)
        # bossSubmit = BOSSCommands.submit(bossJobId,self.bossCfgDir)
        bossSubmit += " -reuseclad "

        try:

          if self.parameters['RTMon']!='':
            bossSubmit+="-rtmon %s "%self.parameters['RTMon']
        except:
          pass
        # // Executing BOSS Submit command
        #//
        # AF : remove the following buggy logging
        logging.debug( "RESubmitter.doSubmit:", bossSubmit)
        output = BOSSCommands.executeCommand(bossSubmit)
        logging.debug ("RESubmitter.doSubmit: %s" % output)
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
          BOSSCommands.FailedSubmission(str(bossJobId),self.bossCfgDir)
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
        #logging.info("Scheduler id from RESubmitter=%s"%jobGridId)
        dashboardInfo['ApplicationVersion'] = self.listToString(self.parameters['AppVersions'])
        dashboardInfo['TargetCE'] = self.listToString(self.parameters['Whitelist'])
        dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
         
        # dashboardInfo.job=dashboardInfo.job+"_"+jobGridId
        dashboardInfo['Scheduler']='RE'
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
      



registerSubmitter(RESubmitter, RESubmitter.__name__)


