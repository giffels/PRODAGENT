#!/usr/bin/env python
"""
_GlideInWMS_

GlideInWMS Submitter plugin

Pulls site information from the ResourceControlDB to match site IDs
to GlideIn desired sites. If no site provided, then plain vanilla submit
to the glide in factory and let it go anywhere.

Note that all merge and cleanup type jobs will have a single destination
site set. Processing jobs may be unrestricted in site or have a list
of sites where they need to go


"""

__revision__ = "$Id: GlideInWMS.py,v 1.7 2008/05/07 18:44:32 sfiligoi Exp $"

import os
import logging
import time
import string

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from JobSubmitter.Submitters.OSGUtils import standardScriptHeader
from JobSubmitter.Submitters.OSGUtils import bulkUnpackerScript
from JobSubmitter.Submitters.OSGUtils import missingJobReportCheck



from ProdAgent.ResourceControl.ResourceControlAPI import createSiteNameMap


class GlideInWMS(BulkSubmitterInterface):
    """
    _GlideInWMS_

    Submitter that takes a job spec (single or bulk) and creates
    submission JDL for the GlideIn WMS.

    Site Data is looked up from the PA side in the ResCon DB.
    

    """
    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """
        logging.debug("%s" % self.primarySpecInstance.parameters)
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']

        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.mainSandboxDir = os.path.dirname(self.mainSandbox)
        self.mainSandboxLink = os.path.join(self.mainSandboxDir,
                                            "SandBoxLink.tar.gz")   
        # this is a workaround to avoid submission failures for pathnames
        # greater than 256 characters.  
        if not os.path.exists(self.mainSandboxLink):
           linkcommand = "ln -s %s %s" % (self.mainSandbox,
                                          self.mainSandboxLink)
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
            self.jsLinkFileName = "JobSpecLink.%s.tar.gz" % int(time.time())
            self.specSandboxLink = os.path.join(
                self.specSandboxDir,self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)   
            linkcommand = "ln -s %s %s" % (
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'],
                self.specSandboxLink)
            logging.debug("making link to jobspec: %s"%linkcommand)
            os.system(linkcommand)
            self.jobInputFiles.append(self.specSandboxLink)
            logging.debug("InputSpecSandbox: %s" % self.specSandboxLink) 
        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:

            self.specSandboxDir = os.path.dirname(
                self.specFiles[self.mainJobSpecName])
            self.jsLinkFileName="JobSpecLink.xml" 
            self.specSandboxLink = os.path.join(self.specSandboxDir,
                                                self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)
            linkcommand="ln -s %s %s" % (
                self.specFiles[self.mainJobSpecName],self.specSandboxLink)
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
        logging.debug("GlideInWMS.doSubmit: %s" % condorSubmit)
        output = self.executeCommand(condorSubmit)
        logging.info("GlideInWMS.doSubmit: %s " % output)
        return



    def initJDL(self):
        """
        _initJDL_

        Make common JDL header
        """
        desiredSites = self.lookupDesiredSites()
        
        inpFiles = []

        inpFiles.extend(self.jobInputFiles)
        inpFileJDL = ""
        for f in inpFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        jdl = []
        jdl.append("universe = vanilla\n")
        # Specify where are the jobs to run
        jdl.append('+DESIRED_Archs="INTEL,X86_64"\n')
        if desiredSites!=None:
            desiredSitesJDL = string.join(list(desiredSites),',')
            jdl.append('+DESIRED_Sites = "%s"\n' % desiredSitesJDL)
            jdl.append("Requirements = stringListMember(GLIDEIN_Site,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs)\n")
        else:
            jdl.append("Requirements = stringListMember(Arch, DESIRED_Archs)\n")
            
        # log which glidein ran the job
        jdl.append('+JOB_Site = "$$(GLIDEIN_Site:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Factory = "$$(GLIDEIN_Factory:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Name = "$$(GLIDEIN_Name:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Schedd = "$$(GLIDEIN_Schedd:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_ProcId = "$$(GLIDEIN_ProcId:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Frontend = "$$(GLIDEIN_Client:Unknown)"\n')
        jdl.append('+JOB_Slot = "$$(Name:Unknown)"\n')

        # log glidein benchmark numbers
        jdl.append('+JOB_Machine_KFlops = "$$(KFlops:Unknown)"\n')
        jdl.append('+JOB_Machine_Mips = "$$(Mips:Unknown)"\n')

        jdl.append("transfer_input_files = %s\n" % inpFileJDL)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append(
            "+ProdAgent_ID = \"%s\"\n" % (
            self.primarySpecInstance.parameters['ProdAgentName'],))

      
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

        placeholderScript = \
"""
echo '<FrameworkJobReport JobSpecID=\"%s\" Name=\"cmsRun1\" WorkflowSpecID=\"%s\" Status=\"Failed\">' > FrameworkJobReport.xml
echo ' <ExitCode Value=\"60998\"/>' >> FrameworkJobReport.xml
echo ' <FrameworkError ExitStatus=\"60998\" Type=\"ErrorBootstrappingJob\">' >> FrameworkJobReport.xml
echo "   hostname=`hostname -f` " >> FrameworkJobReport.xml
echo "   site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
echo " </FrameworkError>"  >> FrameworkJobReport.xml
echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
""" % (jobName, self.workflowName)
        script.append(placeholderScript)

        script.extend(standardScriptHeader(jobName, self.workflowName))
        
        spec_file="$PRODAGENT_JOB_INITIALDIR/%s" % self.jsLinkFileName
        if self.isBulk:
            script.extend(bulkUnpackerScript(spec_file))
        else:
            script.append("JOB_SPEC_FILE=%s\n" %spec_file)
            
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

        Make sure config has what is required for this submitter (if anything)
        
        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)
                             
        
   
     




    def lookupDesiredSites(self):
        """
        _lookupDesiredSites_

        If a whitelist is supplied in the job spec instance,
        match it to a desired site value (ce name) in the rescon DB

        If no whitelist is present, return None to trigger a vanilla
        submission

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lockupDesiredSites:")
        if len(self.whitelist) == 0:
            #  //
            # //  No Preference, return None to trigger vanilla
            #//   submission
            logging.debug("lockupDesiredSites:No Whitelist")
            return None
      
  
        
        #  //
        # // Map to Site Name (desired sites attr) in ResCon DB
        #//
        siteMap = createSiteNameMap()

        #  //
        # // Match a list of desired sites
        #//
        matchedSites = set()
        for sitePref in self.whitelist:
            try:
                intSitePref = int(sitePref)
                sitePref = intSitePref
            except ValueError:
                pass
                
            if sitePref not in siteMap.keys():
                logging.debug("lockupDesiredSites: No match: %s" % sitePref)
                continue
            matchedSites.add(siteMap[sitePref])
            logging.debug("lockupDesiredSites: Matched: %s => %s" % (
                sitePref, matchedSites  )
                          )
            break

        if len(matchedSites) == 0:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.whitelist
            msg += "To any Desired site in ResConDB"
            raise JSException(msg, 
                              ClassInstance = self,
                              Whitelist = self.whitelist)
        return matchedSites
                
            

registerSubmitter(GlideInWMS, GlideInWMS.__name__)
