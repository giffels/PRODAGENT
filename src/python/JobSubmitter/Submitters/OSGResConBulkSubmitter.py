#!/usr/bin/env python
"""
_OSGResConBulkSubmitter_

Globus Universe Condor Submitter implementation.

Pulls site information from the ResourceControlDB instead of the
XML file.


"""

__revision__ = "$Id: OSGResConBulkSubmitter.py,v 1.4 2008/02/12 20:05:46 dmason Exp $"

import os
import logging
import time

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from JobSubmitter.Submitters.OSGUtils import standardScriptHeader
from JobSubmitter.Submitters.OSGUtils import bulkUnpackerScript
from JobSubmitter.Submitters.OSGUtils import missingJobReportCheck



from ProdAgent.ResourceControl.ResourceControlAPI import createCEMap


class OSGResConBulkSubmitter(BulkSubmitterInterface):
    """
    _OSGBulkSubmitter_

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
        logging.debug("<<<<<<<<<<<<<<<<<OSGBulkSubmitter>>>>>>>>>>>>>>..")
        logging.debug("%s" % self.primarySpecInstance.parameters)
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']

        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.mainSandboxDir = os.path.dirname(self.mainSandbox)
        self.mainSandboxLink = os.path.join(self.mainSandboxDir,"SandBoxLink.tar.gz")   
        # this is a workaround to avoid submission failures for pathnames
        # greater than 256 characters.  
        if not os.path.exists(self.mainSandboxLink):
           linkcommand = "ln -s %s %s" % (self.mainSandbox,self.mainSandboxLink)
           logging.debug("making link to sandbox: %s"%linkcommand)
           os.system(linkcommand)
        # then check if the link exists already, if not, make it and 
        # instead insert that...
        # do same with jobspecs...
        
        
        logging.debug("mainSandbox: %s" % self.mainSandbox)
        logging.debug("mainSandboxLink: %s" % self.mainSandboxLink)
        self.specSandboxName = None
        self.singleSpecName = None
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles = []
        self.jobInputFiles.append(self.mainSandboxLink)
        
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.specSandboxDir = os.path.dirname(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jsLinkFileName="JobSpecLink.%s.tar.gz" % int(time.time())
            self.specSandboxLink = os.path.join(self.specSandboxDir,self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)   
            linkcommand="ln -s %s %s" % (self.primarySpecInstance.parameters['BulkInputSpecSandbox'],self.specSandboxLink)
            logging.debug("making link to jobspec: %s"%linkcommand)
            os.system(linkcommand)
            self.jobInputFiles.append(self.specSandboxLink)
            logging.debug("InputSpecSandbox: %s" % self.specSandboxLink) 
        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:

            self.specSandboxDir = os.path.dirname(self.specFiles[self.mainJobSpecName])
            self.jsLinkFileName="JobSpecLink.xml" 
            self.specSandboxLink = os.path.join(self.specSandboxDir,self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)
            linkcommand="ln -s %s %s" % (self.specFiles[self.mainJobSpecName],self.specSandboxLink)
            logging.debug("making link to jobspec: %s"%linkcommand)
            os.system(linkcommand)
            self.jobInputFiles.append(self.specSandboxLink)

            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])
            
        #  //
        # // Start the JDL for this batch of jobs
        #//
        self.jdl = self.initJDL()
        
        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobSpec, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            #  //
            # // For each job to submit, generate a JDL entry
            #//
            self.jdl.extend(
                self.makeJobJDL(jobSpec, cacheDir, self.specFiles[jobSpec]))
            
            
        msg = ""
        for line in self.jdl:
            msg += line

        logging.debug(msg)

        jdlFile = "%s/submit.jdl" % self.toSubmit[self.mainJobSpecName]
        handle = open(jdlFile, 'w')
        handle.writelines(self.jdl)
        handle.close()

        logging.debug("jdl File = %s" % jdlFile)
        

        condorSubmit = "condor_submit %s" % jdlFile
        logging.debug("OSGSubmitter.doSubmit: %s" % condorSubmit)
        output = self.executeCommand(condorSubmit)
        logging.info("OSGSubmitter.doSubmit: %s " % output)
        return



    def initJDL(self):
        """
        _initJDL_

        Make common JDL header
        """
        globusScheduler = self.lookupGlobusScheduler()
        inpFiles = []

        inpFiles.extend(self.jobInputFiles)
        inpFileJDL = ""
        for f in inpFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        jdl = []
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = %s\n" % globusScheduler)
        jdl.append("transfer_input_files = %s\n" % inpFileJDL)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("+ProdAgent_ID = \"%s\"\n" % self.primarySpecInstance.parameters['ProdAgentName'])
        return jdl
    
        
    def makeJobJDL(self, jobID, cache, jobSpec):
        """
        _makeJobJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """
        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        scriptFile = "%s/submit.sh" % cache 
        self.makeWrapperScript(scriptFile, jobID)
        logging.debug("Submit Script: %s" % scriptFile)
        
        jdl = []
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("initialdir = %s\n" % cache)
        jdl.append("Output = condor.out\n")
        jdl.append("Error = condor.err\n")
        jdl.append("Log = condor.log\n")

        
        #  //
        # // Add in parameters that indicate prodagent job types etc
        #//
        jdl.append("+ProdAgent_JobID = \"%s\"\n" % jobID)
        jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.primarySpecInstance.parameters['JobType'])

        jdl.append("Arguments = %s-JobSpec.xml \n" % jobID)
        jdl.append("Queue\n")
        
        return jdl
        

    def makeWrapperScript(self, filename, jobName):
        """
        _makeWrapperScript_

        Make the main executable script for condor to execute the
        job
        
        """
        
        #  //
        # // Generate main executable script for job
        #//
        script = ["#!/bin/sh\n"]
        script.extend(standardScriptHeader(jobName, self.workflowName))
        

        if self.isBulk:
            script.extend(bulkUnpackerScript(self.specSandboxLink))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.jsLinkFileName)   
            
        script.append(
             "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % "SandBoxLink.tar.gz"
            )
        script.append("cd %s\n" % self.workflowName)
        
        
                      
        script.append("./run.sh $JOB_SPEC_FILE\n")
        script.append(
            "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
        script.extend(missingJobReportCheck(jobName))


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
                             

        # expect globus scheduler in OSG block
        # self.pluginConfig['OSG']['GlobusScheduler']
        if not self.pluginConfig.has_key("OSG"):
            msg = "Plugin Config for: %s \n" % self.__class__.__name__
            msg += "Does not contain an OSG config block"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)

        #  //
        # // Validate the value of the GlobusScheduler is present
        #//  and sane
        sched = self.pluginConfig['OSG'].get("GlobusScheduler", None)
        if sched in (None, "None", "none", ""):
            msg = "Invalid value for OSG GlobusScheduler in Submitter "
            msg += "plugin configuration: %s\n" % sched
            msg += "You must provide a valid scheduler"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)
          
        #  //
        # // Extract the mapping of SEName to Jobmanager from the
        #//  plugin config
        siteMapping = self.pluginConfig.get('SENameToJobmanager', None)
        if siteMapping == None:
            msg = "SENameToJobManager not provided in Submitter "
            msg += "pluging configuration\n"
            msg += "This is required for mapping merge jobs to the appropriate"
            msg += "Site based on fileblock name"
            raise JSException(msg, 
                              ClassInstance = self,
                              PluginConfig = self.pluginConfig)
        




    def lookupGlobusScheduler(self):
        """
        _lookupGlobusScheduler_

        If a whitelist is supplied in the job spec instance,
        match it to a globus scheduler using the SENameToJobmanager map.

        If no whitelist is present, the standard OSG GlobusSubmitter is used,

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lookupGlobusScheduler:")
        if len(self.whitelist) == 0:
            #  //
            # //  No Preference, use plain GlobusScheduler
            #//
            logging.debug("lookupGlobusScheduler:No Whitelist")
            return self.pluginConfig['OSG']['GlobusScheduler']
      
  
        
            
        ceMap = createCEMap()
        
        matchedJobMgr = None
        for sitePref in self.whitelist:
            try:
                intSitePref = int(sitePref)
                sitePref = intSitePref
            except ValueError:
                pass
                
            if sitePref not in ceMap.keys():
                logging.debug("lookupGlobusScheduler: No match: %s" % sitePref)
                continue
            matchedJobMgr = ceMap[sitePref]
            logging.debug("lookupGlobusScheduler: Matched: %s => %s" % (
                sitePref, matchedJobMgr  )
                          )
            break

        if matchedJobMgr == None:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.whitelist
            msg += "To any JobManager"
            raise JSException(msg, 
                              ClassInstance = self,
                              SENameToJobmanager = ceMap,
                              Whitelist = self.whitelist)
        return matchedJobMgr
                
            

registerSubmitter(OSGResConBulkSubmitter, OSGResConBulkSubmitter.__name__)
