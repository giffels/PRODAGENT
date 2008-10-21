#!/usr/bin/env python
"""
_OSGGlideIn_

Globus Universe Condor Submitter implementation.

"""

__revision__ = "$Id: OSGGlideIn.py,v 1.5 2008/10/20 18:12:05 gutsche Exp $"

import os
import logging
import time 


from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from JobSubmitter.Submitters.OSGUtils import standardScriptHeader
from JobSubmitter.Submitters.OSGUtils import bulkUnpackerScript
from JobSubmitter.Submitters.OSGUtils import missingJobReportCheck

import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI

class OSGGlideIn(BulkSubmitterInterface):
    """
    _OSGGlideIn_

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
        logging.debug("<<<<<<<<<<<<<<<<<OSGGlideIn>>>>>>>>>>>>>>..")
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
        # // Site details from ResCon
        #//
        #  // glideinsites is the list of site names matching
        # //  the whitelist for the jobs being submitted
        #//   
        self.glideinsites = None
        self.accountingGroup = None
        self.lookupSite()
        

        
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

        jdlFile = "%s/%s-submit.jdl" % (
            self.toSubmit[self.mainJobSpecName],
            self.mainJobSpecName)
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
            

        inpFiles = []
        
        inpFiles.extend(self.jobInputFiles)
        inpFileJDL = ""
        for f in inpFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        jdl = []
        jdl.append("universe = vanilla\n")
        jdl.append("transfer_input_files = %s\n" % inpFileJDL)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("+ProdAgent_ID = \"%s\"\n" % self.primarySpecInstance.parameters['ProdAgentName'])
        jdl.append("+DESIRED_Sites=\"%s\"\n" % self.glideinsites)
        if self.accountingGroup != None:
            jdl.append("+AccountingGroup = \"%s\"\n" % self.accountingGroup)
        jdl.append("+DESIRED_Archs=\"INTEL,X86_64\"\n")
        jdl.append("Requirements = stringListMember(GLIDEIN_Site,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs) \n" )

        #  //
        # // Monitoring/Tracking info
        #// 
        jdl.append("+JOB_Site= \"$$(GLIDEIN_Site:Unknown)\"\n") 
        jdl.append("+JOB_VM=\"$$(Name:Unknown)\"\n") 
        jdl.append("+JOB_Machine_KFlops=\"$$(KFlops:Unknown)\"\n") 
        jdl.append("+JOB_Machine_Mips=\"$$(Mips:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_Schedd=\"$$(GLIDEIN_Schedd:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_ClusterId=\"$$(GLIDEIN_ClusterId:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_ProcId=\"$$(GLIDEIN_ProcId:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_Factory=\"$$(GLIDEIN_Factory:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_Name=\"$$(GLIDEIN_Name:Unknown)\"\n") 
        jdl.append("+JOB_GLIDEIN_Frontend=\"$$(GLIDEIN_Client:Unknown)\" \n") 
        jdl.append('+JOB_GLIDEIN_Gatekeeper = "$$(GLIDEIN_Gatekeeper:Unknown)"\n')
        jdl.append('environment = CONDOR_JOBID=$$([GlobalJobId])\n')
        return jdl
    
        
    def makeJobJDL(self, jobID, cache, jobSpec):
        """
        _makeJobJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """

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
        # // Add in parameters that indicate prodagent job types, priority etc
        #//
        jdl.append("+ProdAgent_JobID = \"%s\"\n" % jobID)
        jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.primarySpecInstance.parameters['JobType'])

        if self.primarySpecInstance.parameters['JobType'].lower() == "merge":
            jdl.append("priority = 10\n")

        if self.primarySpecInstance.parameters['JobType'].lower() == "cleanup":
            jdl.append("priority = 5\n")

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

        script.append("if [ -d \"$OSG_GRID\" ]; then\n")
        script.append("source $OSG_GRID/setup.sh\n")
        script.append("fi\n")
        script.append("echo Starting up OSG prodAgent job\n")
        script.append("echo hostname: `hostname -f`\n")
        script.append("echo site: $OSG_SITE_NAME\n")
        script.append("echo gatekeeper: $OSG_JOB_CONTACT\n")
        script.append("echo pwd: `pwd`\n")
        script.append("echo date: `date`\n")
        script.append("echo df output:\n")
        script.append("df\n")
        script.append("echo env output:\n")
        script.append("env\n")
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        

        if self.isBulk:
            #script.extend(bulkUnpackerScript(self.specSandboxLink))
            script.extend(bulkUnpackerScript(os.path.basename(self.specSandboxLink)))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.jsLinkFileName)
        
            
        script.append(
#            "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % self.mainSandboxName
            "tar -zxf $PRODAGENT_JOB_INITIALDIR/SandBoxLink.tar.gz\n" 
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
        #if not self.pluginConfig.has_key("OSG"):
        #    msg = "Plugin Config for: %s \n" % self.__class__.__name__
        #    msg += "Does not contain an OSG config block"
        #    raise JSException( msg , ClassInstance = self,
        #                       PluginConfig = self.pluginConfig)

        #  //
        # // Validate the value of the GlobusScheduler is present
        #//  and sane
        #sched = self.pluginConfig['OSG'].get("GlideInSite", None)
        #if sched in (None, "None", "none", ""):
        #    msg = "Invalid value for OSG GlideInSite in Submitter "
        #    msg += "plugin configuration: %s\n" % sched
        #    msg += "You must provide a valid default glidein site"
        #    raise JSException( msg , ClassInstance = self,
        #                       PluginConfig = self.pluginConfig)
        
        
        



    def lookupSite(self):
        """
        _lookupSite_

        If a whitelist is supplied in the job spec instance,
        match it to a glidein site name.

        If no whitelist is present, panic

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lookupSite:")
        if len(self.whitelist) == 0:
            #  //
            # //  No Preference, throw
            #//   What to do if there is no site pref?? leave blank??
            logging.error("lookupSite:No Whitelist")
            raise RuntimeError, msg


        ceMap = ResConAPI.createCEMap()
        siteIndexMap = ResConAPI.createSiteIndexMap()
        matchedJobMgrs = set()
        for sitePref in self.whitelist:
            try:
                intSitePref = int(sitePref)
                sitePref = intSitePref
            except ValueError:
                sitePref = siteIndexMap.get(sitePref, None)

            if sitePref == None:
                msg = "Unable to translate site preference %s\n"
                msg += "into a known site index\n"
                msg += "Known site identifiers: %s\n" % siteIndexMap.keys()
                logging.debug(msg)
                continue
            
            if sitePref not in ceMap.keys():
                logging.debug("lookupSite: No match: %s" % sitePref)
                continue
            matchedJobMgrs.add(ceMap[sitePref])
            logging.debug("lookupSite: Matched: %s => %s" % (
                sitePref, ceMap[sitePref]  )
                          )
            if self.accountingGroup == None:
                siteAttrs = ResConAPI.attributesByIndex(sitePref)
                self.accountingGroup = siteAttrs.get("accounting-group", None)
                

        
        
        if len(matchedJobMgrs) == 0:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.whitelist
            msg += "To any Glidein sites"
            raise JSException(msg, 
                              ClassInstance = self,
                              CEMap = ceMap,
                              Whitelist = self.whitelist)

        sitesString = ""
        for site in matchedJobMgrs:
            sitesString += "%s," % site
        sitesString = sitesString[:-1]
        logging.debug("Site Reqs: %s" % sitesString)

        
        
        self.glideinsites = "FNAL"
        return 


      
    

registerSubmitter(OSGGlideIn, OSGGlideIn.__name__)
