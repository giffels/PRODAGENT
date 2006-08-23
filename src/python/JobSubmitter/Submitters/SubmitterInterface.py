#!/usr/bin/env python
"""
_SubmitterInterface_

Common Submitter Interface, Submitter implementations should inherit this
class.

Submitters should not take any ctor args since they will be instantiated
by a factory

"""
__revision__ = "$Id: SubmitterInterface.py,v 1.17 2006/08/11 14:16:15 bacchi Exp $"

import os
import logging
import time
from popen2 import Popen4

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ProdAgentException import ProdAgentException

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo

class SubmitterInterface:
    """
    _SubmitterInterface_

    General interface for submitter implementation.
    Submitters should inherit this class and implement the
    doSubmit method.


    """
    def __init__(self):
        self.parameters = {}
        self.parameters['NoRecreate'] = True

        # Determine the location of the BOSS configuration files. These is 
        # expected to be in a specific location under the prodAgent workdir, 
        # i.e. <prodAgentWorkDir>/bosscfg 
        # AF: this is nolonger true.... pick up configdir from BOSS configDir
        cfgObject = loadProdAgentConfiguration()
        prodAgentConfig = cfgObject.get("ProdAgent")
        workingDir = prodAgentConfig['ProdAgentWorkDir'] 
        workingDir = os.path.expandvars(workingDir)
        #AFself.bossCfgDir = workingDir + "/bosscfg/"
        bossConfig = cfgObject.get("BOSS")
        self.bossCfgDir = bossConfig['configDir']
        
        #  //
        # // Load plugin configuration
        #//
        self.pluginConfig = None
        try:
            #  //
            # // Always searches in JobSubmitter Config Block
            #//  for parameter called SubmitterPluginConfig
            self.pluginConfig = loadPluginConfig("JobSubmitter",
                                                 "Submitter")
        except StandardError, ex:
            msg = "Failed to load Plugin Config for Submitter Plugin:\n"
            msg += "Plugin Name: %s\n" % self.__class__.__name__
            msg += str(ex)
            logging.warning(msg)
            
        self.checkPluginConfig()

    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Override this method to check/set defaults etc for the
        Plugin config for the Submitter Plugin being used

        If self.pluginConfig == None, there was an error loading the config
        or it was not found

        """
        pass
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_
        
        Invoke some command that submits the job.

        Arguments are the location of the wrapper script to submit the job
        with as an executable, and the location of the jobTarball that
        contains the actual job guts.
        
        """
        msg =  "Virtual Method SubmitterInterface.doSubmit called"
        raise RuntimeError, msg
        
    def editDashboardInfo(self, dashboardInfo):
        """
        _editDashboardInfo_

        Add data about submission to DashboardInfo dictionary before
        it is published to the dashboard

        A default set of information will be sent if this method is not
        overridden.

        If dashboardInfo is None, it is not available for this job.

        """
        pass
    

    
    def generateWrapper(self, wrapperName, tarball, jobname):
        """
        _generateWrapper_

        Generate an execution wrapper for the job. Arguments provided
        are the name of the job Tarball and the name of the executable
        within that tarball.

        - *wrapperName* : Location of the script, create this file

        - *tarball* : Complete path to the tarball in its
        presubmission location

        - *jobname* : Name of the job. this is also the name of the top
        level dir in the tarfile where the job should be executed from.

        The main script for the job is always <jobname>/run.sh and you
        should invoke run.sh from the directory in which it resides

        This base class provides a very simple wrapper generation mechanism
        that builds a bash script
        
        """
        script = ["#!/bin/sh\n"]
        script.append("tar -zxf %s\n" % os.path.basename(tarball))
        script.append("cd %s\n" % jobname)
        script.append("./run.sh\n")
        
        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()

        return 
        

    def __call__(self, workingDir, jobCreationArea, jobname, **args):
        """
        _Operator()_

        Take a working area, job area and job name and generate
        a tarball in the working area containing the jobCreationArea
        contents.
        
        Invoke the overloaded methods to handle wrapper creation and
        submission for that job

        """
        logging.debug("SubmitterInterface.__call__")
        logging.debug("Subclass:%s" % self.__class__.__name__)
        self.parameters.update(args)

        #  //
        # // Check to see Job tarball exists, if not create it, if
        #//  so, then check to see if it needs recreating
        tarballName = self.tarballName(workingDir, jobname)
        tarballExists = os.path.exists(tarballName)
        if not tarballExists:
            tarball = createTarball(workingDir, jobCreationArea, jobname)
        else:
            #  //
            # // Tarball exists: Do we need to recreate it?
            #//
            if not self.parameters.get("NoRecreate", False):
                #  //
                # // NoRecreate is False, so we recreate 
                #//
                tarball = createTarball(workingDir, jobCreationArea, jobname)
            else:
                tarball = tarballName 

        #  //
        # // After creation of the tarball, cleanup the job area used to
        #//  make it.
        #  //
        # // Subclasses can disable this by adding
        #//  self.parameters["CleanTarInput"] = False to their ctor
        if self.parameters.get("CleanTarInput", True):
            #  //
            # // Clean up tar input.
            #//
            logging.debug("SubmitterInterface:Cleaning Tar Input")
            logging.debug("Removing: %s" % jobCreationArea)
            os.system("/bin/rm -rf %s" % jobCreationArea)
            
            
                
        wrapperName = os.path.join(workingDir, "%s-submit" % jobname)
        logging.debug("SubmitterInterface:Tarball=%s" % tarball)
        logging.debug("SubmitterInterface:Wrapper=%s" % wrapperName)

        #  //
        # // save file and directory information for subclasses use (Carlos)
        #//
        self.baseDir = os.path.dirname(os.path.dirname(jobCreationArea))
        self.executableFile = "%s-submit" % os.path.dirname(jobCreationArea)

        #  //
        # // Generate some standard parameters
        #//
        self.parameters['BaseDir'] = os.path.dirname(
            os.path.dirname(jobCreationArea)
            )
        self.parameters['ExecutableFile'] = "%s-submit" % (
            os.path.dirname(jobCreationArea),
            )
        self.parameters['JobCacheArea'] = jobCreationArea
        self.parameters['JobName'] = jobname
        self.parameters['Tarball'] = tarballName
        self.parameters['Wrapper'] = wrapperName
        self.parameters['AppVersions'] = \
                   self.parameters['JobSpecInstance'].listApplicationVersions()
        self.parameters['Blacklist'] = \
                   self.parameters['JobSpecInstance'].siteBlacklist
        self.parameters['Whitelist'] = \
                   self.parameters['JobSpecInstance'].siteWhitelist


        self.parameters['DashboardInfo'] = None
        self.parameters['DashboardID'] = None
        dashboardInfoFile = os.path.join(
            os.path.dirname(jobCreationArea), 'DashboardInfo.xml')
        if os.path.exists(dashboardInfoFile):
            dashboardInfo = DashboardInfo()
            dashboardInfo.read(dashboardInfoFile)
            dashboardInfo.job = "%s_%s" % (dashboardInfo.job, time.time())
            dashboardInfo['Scheduler'] = self.__class__.__name__
            self.parameters['DashboardInfo'] = dashboardInfo
            self.parameters['DashboardID'] = dashboardInfo.job
            
        
              
        
        bossId = self.isBOSSDeclared()
        if bossId != None:
            self.parameters['BOSSID'] = bossId
            

        #  //
        # // Generate a wrapper script
        #//
        self.generateWrapper(wrapperName, tarball, jobname)


        #  //
        # // Invoke Hook to edit the DashboardInfo with submission 
        #//  details in plugins and then publish the dashboard info.
        self.editDashboardInfo(self.parameters['DashboardInfo'])
        self.publishSubmitToDashboard(self.parameters['DashboardInfo'])
        
        #  //
        # // Invoke whatever is needed to do the submission
        #//
        self.doSubmit(wrapperName, tarball)
        
        return

    def executeCommand(self, command):
        """
        _executeCommand_

        Util it execute the command provided in a popen object

        """
        logging.debug("SubmitterInterface.executeCommand:%s" % command)
        pop = Popen4(command)
        while pop.poll() == -1:
            exitCode = pop.poll()
        exitCode = pop.poll()

        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "Exited with code: %s\n" % exitCode
            msg += pop.fromchild.read()
            logging.error("SubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        return pop.fromchild.read()
    

    def tarballName(self, targetDir, jobName):
        """
        _tarballName_

        What is the name of the tarball for this job being submitted,
        including path?

        """
        return os.path.join(targetDir, "%s.tar.gz" % jobName)

    def publishSubmitToDashboard(self, dashboardInfo):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        NOTE: should probably read destination from cfg file, hardcoding
        it here for time being.

        """
        if dashboardInfo == None:
            logging.debug(
                "SubmitterInterface: No DashboardInfo available for job"
                )
            return
        dashboardInfo['ApplicationVersion'] = listToString(self.parameters['AppVersions'])
        dashboardInfo['TargetCE'] = listToString(self.parameters['Whitelist'])
        dashboardInfo.addDestination("lxgate35.cern.ch", 8884)
        dashboardInfo.publish(5)
        return
        

    def declareToBOSS(self):
        """
        _declareToBOSS_

        Declare this job to BOSS.
        Parameters are extracted from this instance

        """
                                   
 
        logging.debug("SubmitterInterface:Declaring Job To BOSS")
        bossJobId=self.BOSS4declare()

        ## move id file out from job-cache area
        #idFile = "%s/%sid" % (
        #    self.parameters['JobCacheArea'], self.parameters['JobName'],
        #    )
        idFile = "%s/%sid" % (os.path.dirname(self.parameters['Wrapper']), self.parameters['JobName'])

        handle = open(idFile, 'w')
        handle.write("JobId=%s" % bossJobId)
        handle.close()
        logging.debug("SubmitterInterface:BOSS JobID File:%s" % idFile)
        #os.remove(cladfile)
        return

    def isBOSSDeclared(self):
        """
        _isBOSSDeclared_

        If this job has been declared to BOSS, return the BOSS ID
        from the cache area. If it has not, return None

        """
        ## move id file out from job-cache
        #idFile = "%s/%sid" % (
        #    self.parameters['JobCacheArea'], self.parameters['JobName'],
        #    )
        idFile ="%s/%sid" % (os.path.dirname(self.parameters['Wrapper']), self.parameters['JobName'])

        if not os.path.exists(idFile):
            #  //
            # // No BOSS Id File ==> not declared
            #//
            return None
        content = file(idFile).read().strip()
        content=content.replace("JobId=", "")
        try:
            jobId = int(content)
        except ValueError:
            jobId = None
        return jobId
        

        
    def BOSS4declare(self):
        """
        BOSS4declare

        BOSS 4 command to declare a task
        """
        
        bossQuery = "bossAdmin SQL -query \"select id from PROGRAM_TYPES "
        bossQuery += "where id = 'cmssw'\" -c " + self.bossCfgDir
        queryOut = self.executeCommand(bossQuery)
        bossJobType = "cmssw"
        if queryOut.find("cmssw") < 0:
            bossJobType=""
        

        logging.debug( "bossJobType = %s"%bossJobType)
        ## move one dir up 
        #xmlfile = "%s/%sdeclare.xml" % (
        #    self.parameters['JobCacheArea'], self.parameters['JobName'],
        #    )
        xmlfile = "%s/%sdeclare.xml"% (
            os.path.dirname(self.parameters['Wrapper']) , self.parameters['JobName']
            )
        logging.debug( "xmlfile=%s"%xmlfile)
        bossDeclare = "boss declare -xmlfile %s"%xmlfile + "  -c " + self.bossCfgDir
        declareClad=open(xmlfile,"w")
        declareClad.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n")
        
        declareClad.write("<task name=\"%s\">"%self.parameters['JobName'])
        declareClad.write("<chain scheduler=\"%s\" rtupdater=\"mysql\" ch_tool_name=\"\">"%self.parameters['Scheduler'])
        declareClad.write(" <program exec=\"%s\" args=\"\" stderr=\"%s.stderr\" program_types=\"%s\" stdin=\"\" stdout=\"%s.stdout\"  infiles=\"%s,%s\" outfiles=\"*.root,%s.stdout,%s.stderr,FrameworkJobReport.xml\"  outtopdir=\"\"/></chain></task>"% (os.path.basename(self.parameters['Wrapper']), self.parameters['JobName'],bossJobType, self.parameters['JobName'],self.parameters['Wrapper'],self.parameters['Tarball'], self.parameters['JobName'], self.parameters['JobName']))
        declareClad.close()
        logging.debug("SubmitterInterface:BOSS xml declare file written:%s" % xmlfile)

        #  //
        # // Do BOSS Declare
        #//
        bossJobId = self.executeCommand(bossDeclare)
        logging.debug( bossJobId)
        try:
            bossJobId = bossJobId.split("TASK_ID:")[1].split("\n")[0].strip()
        except StandardError, ex:
            logging.debug("SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" % bossJobId)
            raise ProdAgentException("Job Declaration Failed")
        #os.remove(xmlfile)
        return bossJobId
        


        


    def BOSS4submit(self,bossJobId):
        """
        BOSS4submit

        BOSS 4 command to submit a task
        """
        bossSubmit = "boss submit "
        bossSubmit += "-taskid %s " % bossJobId
        bossSubmit += "-scheduler %s " %  self.parameters['Scheduler']
        bossSubmit += " -c " + self.bossCfgDir + " "
        return bossSubmit
                


def listToString(listInstance):
    """
    _listToString_

    Lists to string conversion util for Dashboard formatting

    """
    result = str(listInstance)
    result = result.replace('[', '')
    result = result.replace(']', '')
    result = result.replace(' ', '')
    result = result.replace('\'', '')
    return result

def createTarball(targetDir, sourceDir, tarballName):
    """
    _createTarball_

    Create a tarball in targetDir named tarballName.tar.gz containing
    the contents of sourceDir.

    Return the path to the resulting tarball

    """
    logging.debug("SubmitterInterface.createTarball")
    logging.debug("createTarball:Target=%s" % targetDir)
    logging.debug("createTarball:Source=%s" % sourceDir)
    logging.debug("createTarball:Tarball=%s" % tarballName)
    tarballFile = os.path.join(targetDir, "%s.tar.gz" % tarballName)
    if os.path.exists(tarballFile):
        logging.debug(
            "createTarball:Tarball exists, cleaning: %s" % tarballFile)
        os.remove(tarballFile)
    
    tarComm = "tar -czf %s -C %s %s " % (
        tarballFile,
        os.path.dirname(sourceDir),
        os.path.basename(sourceDir)
        )

    pop = Popen4(tarComm)
    while pop.poll() == -1:
            exitCode = pop.poll()
    exitCode = pop.poll()

    if exitCode:
        msg = "Error creating Tarfile:\n"
        msg += tarComm
        msg += "Exited with code: %s\n" % exitCode
        msg += pop.fromchild.read()
        logging.error("createTarball: Tarball creation failed:")
        logging.error(msg)
        raise RuntimeError, msg
    return tarballFile

    
    
    
