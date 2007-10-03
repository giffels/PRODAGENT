#!/usr/bin/env python
"""
_RESubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: RESubmitter.py,v 1.4 2007/03/15 09:32:54 bacchi Exp $"

#  //
# // Configuration variables for this submitter
#//
#bossJobType = ""  # some predetermined type value from boss install here
bossScheduler = "glite"

#  //
# // End of Config variables.
#//
import os
import logging
import exceptions
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentBOSS import BOSSCommands

class InvalidFile(exceptions.Exception):
    def __init__(self, msg):
        args = "%s\n" % msg
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
        
        self.bossStrJobId = ""
        self.parameters['Scheduler'] = "glite"
         # check for dashboard usage
        self.usingDashboard = {'use' : 'True', \
                               'address' : 'lxgate35.cern.ch', \
                               'port' : '8884'}
        try:
            pluginConfig = loadPluginConfig("JobSubmitter", "Submitter")
            dashboardCfg = pluginConfig.get('Dashboard', {})
            self.usingDashboard['use'] = dashboardCfg.get(
                "UseDashboard", "False"
                )
            self.usingDashboard['address'] = dashboardCfg.get(
                "DestinationHost"
                )
            self.usingDashboard['port'] = dashboardCfg.get("DestinationPort")
            logging.debug("DashboardInfo = %s" % dashboardInfo.__str__())
        except:
            logging.info("No Dashboard section in SubmitterPluginConfig")


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
        logging.info("jobname = %s" % jobname)
        tmp = jobname.split('_')
        chainId = tmp[len(tmp) - 1]
        taskName = '_'.join(tmp[:len(tmp) - 1])
        logging.info("TaskName = %s" % taskName)
        taskId = BOSSCommands.getTaskIdFromName(taskName, self.bossCfgDir)
        logging.info("TaskId = %s" % taskId)
        self.parameters['TaskId'] = taskId + "." + chainId
        self.parameters['JobName'] = '_'.join(tmp[:len(tmp) - 1])
        logging.info("taskid = %s" % self.parameters['TaskId'])
      
        self.doSubmit("", "")
      
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        
        Override Submission action to construct a BOSS submit command
        and run it

        Initial tests: No FrameworkJobReport yet, stage back stdout log
        
        """
        logging.info("taskid%s" % self.parameters['TaskId'])

        try:
            taskid = self.parameters['TaskId'].split('.')[0]
            chainid = self.parameters['TaskId'].split('.')[1]
            int(taskid)
            int(chainid)
            bossJobId = self.parameters['TaskId']
        except:
            bossJobId = None
            # bossJobId=self.isBOSSDeclared()

        logging.info("bossJobId%s" % bossJobId)
        logging.debug( "RESubmitter.doSubmit bossJobId = %s" % bossJobId)
        if bossJobId == None:
            raise ProdAgentException("Failed Job Declaration")
            
        ( bossSubmit, cert ) = \
          BOSSCommands.resubmit(bossJobId, self.bossCfgDir)
        
        logging.info("Using certificate : " + cert)
        try:
            output = BOSSCommands.executeCommand(
                "voms-proxy-info", userProxy = cert
                )
            output = output.split("timeleft")[1].strip()
            output = output.split(":")[1].strip()
            if output == "0:00:00":
                #logging.info( "You need a voms-proxy-init -voms cms")
                logging.error("voms-proxy-init expired")
                #sys.exit()
        except StandardError,ex:
            #print "You need a voms-proxy-init -voms cms"
            logging.error("voms-proxy-init does not exist")
            logging.error(output)
            raise ProdAgentException("Proxy Expired")
        # sys.exit()

# // useless!!!
#        try:
#          if self.parameters['RTMon']!='':
#            bossSubmit+="-rtmon %s "%self.parameters['RTMon']
#        except:
#          pass


        # // Executing BOSS Submit command
        #//
        logging.debug( "RESubmitter.doSubmit:", bossSubmit)
        output = BOSSCommands.executeCommand(bossSubmit, userProxy = cert)
        logging.debug ("RESubmitter.doSubmit: %s" % output)
        if output.find("error") >= 0:
            BOSSCommands.FailedSubmission(str(bossJobId), self.bossCfgDir)
            raise ProdAgentException("Submission Failed")
        #os.remove(cladfile)
        try:
            resub = \
                  output.split("Resubmission number")[1].split("\n")[0].strip()
            logging.debug("resub = %s" % resub)
        except:
            resub = "1"
        try:
            chainid = \
                    (output.split("Scheduler ID for job")[1]).split("is")[0].strip()
        except:
            BOSSCommands.FailedSubmission(str(bossJobId), self.bossCfgDir)
            raise ProdAgentException("Submission Failed")

        self.bossStrJobId = str(bossJobId) + "." + chainid + "." + resub
        logging.info("Submitter bossJobId = %s" % bossJobId)
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

        if  self.usingDashboard['use'] != 'True':
            return
        
        jobSpecId = BOSSCommands.jobSpecId(jobId, self.bossCfgDir)

        ( dashboardInfo, dashboardInfoFile ) = \
          BOSSCommands.guessDashboardInfo(jobId, jobSpecId, self.bossCfgDir)
        if dashboardInfo.task == '' :
            logging.error( "unable to retrieve DashboardId" )
            return
        
        dashboardInfo.job = chainid + '_' + schedulerI['SCHED_ID']
        dashboardInfo['ApplicationVersion'] = self.listToString(
            self.parameters['AppVersions']
            )
        dashboardInfo['TargetCE'] = self.listToString(
            self.parameters['Whitelist']
            )
        dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
        dashboardInfo['Scheduler'] = 'RE'
        dashboardInfo['GridJobID'] = \
                                   BOSSCommands.schedulerId(jobId, bossCfgDir)

        # set dashboard destination
        dashboardInfo.addDestination(
            self.usingDashboard['address'], self.usingDashboard['port']
            )

        dashboardInfo.write( dashboardInfoFile )
        dashboardInfo.publish(5)
            
        return
      



registerSubmitter(RESubmitter, RESubmitter.__name__)


