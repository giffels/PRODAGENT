#!/usr/bin/env python
"""
_RESubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: RESubmitter.py,v 1.4.12.2 2007/10/03 18:14:03 gcodispo Exp $"
__version__ = "$Revision: 1.4.12.2 $"
#  //
# // Configuration variables for this submitter
#//

#  //
# // End of Config variables.
#//
import os
import time
import logging
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentBOSS import BOSSCommands
from ProdAgent.WorkflowEntities.JobState import doNotAllowMoreSubmissions

class RESubmitter(SubmitterInterface):
    """
    _RESubmitter_
    
    Simple BOSS Submission wrapper for testing.

    """
    
    def __init__(self):
        
        SubmitterInterface.__init__(self)
        
        #  //
        # // BOSS job variables
        #//
        self.taskId  = ''
        self.chainId = ''
        self.bossJobId = ''
        self.parameters['Scheduler'] = "glite"

         # check for dashboard usage
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
        except StandardError:
            logging.info("No Dashboard section in SubmitterPluginConfig")


    def checkPluginConfig(self):
        """
        _checkPluginConfig_
        
        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            
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
        self.parameters['JobName'] = jobname
        logging.info("JobName = %s" % self.parameters['JobName'])

        # get taskname
        sep = jobname.rfind( '_' )
        taskName = jobname[:sep]  
        self.parameters['TaskName'] = taskName
        logging.info("TaskName = %s" % self.parameters['TaskName'])

         # get BOSS id     
        self.chainId  = jobname[sep+1:]
        self.taskId = BOSSCommands.getTaskIdFromName(taskName, self.bossCfgDir)
      
        self.bossJobId = self.taskId + "." + self.chainId
        logging.info("BOSS id = %s" % self.bossJobId)
        
        self.doSubmit("", "")
      
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        
        Override Submission action to construct a BOSS submit command
        and run it

        Initial tests: No FrameworkJobReport yet, stage back stdout log
        
        """

#        logging.info("taskid%s" % self.parameters['TaskId'])
#        try:
#            self.taskid = self.parameters['TaskId'].split('.')[0]
#            self.chainId = self.parameters['TaskId'].split('.')[1]
#            int(self.taskid)
#            int(self.chainId)
#            self.bossJobId = self.parameters['TaskId']
#        except StandardError :
#            self.bossJobId = None
            # self.bossJobId=self.isBOSSDeclared()

        logging.debug( "RESubmitter.doSubmit bossJobId = %s" % self.bossJobId)
        if self.bossJobId == None:
            raise ProdAgentException( "Failed Job Declaration", \
                               self.parameters['JobName'] )
            
        ( bossSubmit, cert ) = \
          BOSSCommands.resubmit(self.bossJobId, self.bossCfgDir)

        if cert != '' :
            logging.info("Using certificate : " + cert)

        # // Check proxy validity: an exception raised will stop the submission
        #//
        try:
            BOSSCommands.checkUserProxy( cert )
        except ProdAgentException, ex :
            try:
                doNotAllowMoreSubmissions([ self.parameters['JobName'] ])
                raise
            except ProdAgentException, ex:
                msg = "Updating max racers fields failed for job %s\n" \
                      % jobSpecId
                msg += str(ex)
                logging.error(msg)
                raise

        # // Executing BOSS Submit command
        #//
        logging.debug( "RESubmitter.doSubmit:", bossSubmit)
        output = BOSSCommands.executeCommand(bossSubmit, userProxy = cert)
        logging.debug ("RESubmitter.doSubmit: %s" % output)
        if output.find("error") >= 0:
            logging.error ("RESubmitter.doSubmit: %s" % output)
            BOSSCommands.FailedSubmission(self.bossJobId, self.bossCfgDir)
            raise ProdAgentException( "Submission Failed" )

        try:
            resub = \
                  output.split("Resubmission number")[1].split("\n")[0].strip()
            logging.debug("resub = %s" % resub)
        except StandardError :
            resub = "1"
        try:
            chainId = \
                    (output.split("Scheduler ID for job")[1]).split("is")[0].strip()
        except StandardError :
            logging.error ("RESubmitter.doSubmit: %s" % output)
            BOSSCommands.FailedSubmission(self.bossJobId, self.bossCfgDir)
            raise ProdAgentException( "Missing BOSS job id" )

        if chainId != self.chainId :
            strerr = "mismatching job id: " +  chainId + " != " + self.chainId
            logging.error( strerr )
            raise ProdAgentException( strerr )
        
        self.bossJobId = self.bossJobId + "." + resub
        logging.info("Submitter bossJobId = %s" % self.bossJobId)
        try :
            self.parameters['DashboardInfo'] = None
            self.editDashboardInfo(self.parameters['DashboardInfo'])
            self.publishSubmitToDashboard(self.parameters['DashboardInfo'])
        except StandardError, msg:
            logging.error("Cannot publish dashboard information: " + \
                          self.parameters['DashboardInfo'].__str__() + \
                          "\n" + str(msg))
        return



    def publishSubmitToDashboard(self, dashboardInfo):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        """
        if dashboardInfo == None:
            logging.debug(
                "SubmitterInterface: No DashboardInfo available for job"
                )
            return

        # set dashboard destination
        logging.debug("dashboardinfo: %s" % dashboardInfo.__str__())
        dashboardInfo.addDestination(
            self.usingDashboard['address'], self.usingDashboard['port']
            )

        dashboardInfo.publish(5)
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
        
        jobSchedId = BOSSCommands.schedulerId(self.bossJobId, self.bossCfgDir)
        
        logging.info( self.parameters['JobName'] + '\n' + \
                      self.parameters['TaskName'] + '\n' + \
                      self.bossJobId + '\n' + \
                      jobSchedId + '\n' )
        
        ( dashboardInfo, dashboardInfoFile ) = BOSSCommands.guessDashboardInfo(
            self.bossJobId, self.parameters['JobName'], self.bossCfgDir
            )
        
        # assign job dashboard id
        if dashboardInfo.task == '' :
            logging.error( "unable to retrieve DashboardId" )
            return
        
        # job basic information
        dashboardInfo.job = self.chainId + '_' + jobSchedId
        dashboardInfo['JSToolUI'] = os.environ['HOSTNAME']
        dashboardInfo['Scheduler'] = self.__class__.__name__
        dashboardInfo['GridJobID'] = jobSchedId
        dashboardInfo['SubTimeStamp'] = time.strftime( '%Y-%m-%d %H:%M:%S' )
        
#        dashboardInfo['SubTimeStamp'] = time.strftime( \
#                             '%Y-%m-%d %H:%M:%S', \
#                             time.gmtime(float(schedulerI['LAST_T'])))

#        # job requirements (not yet implemented
#         try :
#             dashboardInfo['ApplicationVersion'] = self.listToString(
#                 self.parameters['AppVersions']
#                 )
#         except KeyError:
#             logging.info ( "missing AppVersions" )
#         try :
#             dashboardInfo['TargetCE'] = self.listToString(
#                 self.parameters['Whitelist']
#                 )
#         except KeyError:
#             logging.info ("missing Whitelist" )

        # write dashboard file
        dashboardInfo.write( dashboardInfoFile )
        logging.info("Created dashboardInfoFile " + dashboardInfoFile )

        return
      



registerSubmitter(RESubmitter, RESubmitter.__name__)


