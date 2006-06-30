#!/usr/bin/env python
"""
_OSGSubmitter_

Globus Universe Condor Submitter implementation.
Used for testing jobs in a batch environment, shouldnt be used generally
as it includes no job tracking.


"""

__revision__ = "$Id: OSGSubmitter.py,v 1.3 2006/06/28 19:04:52 evansde Exp $"

import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException



class OSGSubmitter(SubmitterInterface):
    """
    _OSGSubmitter_

    Globus Universe condor submitter. Generates a simple JDL file
    and condor_submits it using a dag wrapper and post script to generate
    a callback to the ProdAgent when the job completes.
    

    """
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
            msg += "plugin configuration: %\n" % sched
            msg += "You must provide a valid scheduler"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)
            
        

    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        _generateWrapper_

        Use the default wrapper provided by the base class but
        overload this method to also generate a JDL file

        """
        logging.debug("OSGSubmitter.generateWrapper")
        jdlFile = "%s.jdl" % wrapperName
        logging.debug("OSGSubmitter.generateWrapper: JDL %s" % jdlFile)
        directory = os.path.dirname(wrapperName)
        jdl = []
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = %s\n" % self.pluginConfig['OSG']['GlobusScheduler'])
        jdl.append("initialdir = %s\n" % directory)
        jdl.append("Executable = %s\n" % wrapperName)
        jdl.append("transfer_input_files = %s\n" % tarballName)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("Output = %s-condor.out\n" % jobname)
        jdl.append("Error = %s-condor.err\n" %  jobname)
        jdl.append("Log = %s-condor.log\n" % jobname)
        jdl.append("notification = NEVER\n")

        if self.pluginConfig['OSG']['GlobusScheduler'].endswith("jobmanager-pbs"):
            jdl.append("GlobusRSL=(jobtype=single)\n")
        
        jdl.append("Queue\n")
        
        
        handle = open(jdlFile, 'w')
        handle.writelines(jdl)
        handle.close()

        
  
        tarballBaseName = os.path.basename(tarballName)
        script = ["#!/bin/sh\n"]
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("cd $_CONDOR_SCRATCH_DIR\n")
        script.append(
            "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % tarballBaseName 
            )
        script.append("cd %s\n" % jobname)
        script.append("./run.sh\n")
        script.append(
            "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
        script.append("if [ -e $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml ]; then echo 1; else touch $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml; fi; ")
        
        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()
        return
    

    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        Build and run a condor_submit command

        """
        jdlFile = "%s.jdl" % wrapperScript
        
        command = "condor_submit %s" % jdlFile
        logging.debug("OSGSubmitter.doSubmit: %s" % command)
        output = self.executeCommand(command)
        logging.info("OSGSubmitter.doSubmit: %s " % output)
        return
    

registerSubmitter(OSGSubmitter, OSGSubmitter.__name__)
