#!/usr/bin/env python
"""
_BOSSCondorGSubmitter_

Globus Universe Condor Submitter via BOSS implementation.


"""

__revision__ = "$Id: CondorGSubmitter.py,v 1.2 2006/05/02 12:31:14 elmer Exp $"

import os
import logging
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException

import exceptions


class MissingConfigurationKey(exceptions.Exception):
  def __init__(self,msg):
   args="%s\n"%msg
   exceptions.Exception.__init__(self, args)
   pass


class BOSSCondorGSubmitter(SubmitterInterface):
    """
    _BOSSCondorGSubmitter_

    Globus Universe condor submitter. Generates a simple JDL file
    and condor_submits it.
    

    """

    def __init__(self):
        SubmitterInterface.__init__(self)
        #  //
        # // BOSS installation consistency check.
        #//
        if not os.environ.has_key("BOSSDIR"):
            msg = "Error: BOSS environment BOSSDIR not set:\n"
            raise RuntimeError, msg

        # Hard-code this for now, as a 2nd step will remove support for v3
        self.BossVersion = "v4"

        # BOSS supported versions (best red from configration)
        supportedBossVersions = ["v3","v4"]


        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(self.BossVersion):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        
        self.parameters['Scheduler']="condor_g"
        self.bossSubmitCommand={"v3":self.BOSS3submit,"v4":self.BOSS4submit}
        logging.debug("BOSSCondirGSubmitter initialized")

    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)
                               

        # expect globus scheduler in BOSSCondorG block
        # self.pluginConfig['OSG']['GlobusScheduler']
        if not self.pluginConfig.has_key("OSG"):
            msg = "Plugin Config for: %s \n" % self.__class__.__name__
            msg += "Does not contain an BOSSCondorG config block"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)

        if self.pluginConfig['OSG'].get('GlobusScheduler') in (
            "None", None, "",
            ):
            msg = "BOSSCondorG GlobusScheduler is not set!!!!\n"
            msg += "You need to set this in the SubmitterPluginConfig file"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)
        
        

    

    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        _generateWrapper_

        Use the default wrapper provided by the base class but
        overload this method to also generate a JDL file

        """
##         jdlFile = "%s.jdl" % wrapperName
##         print "BOSSCondorGSubmitter.generateWrapper:", jdlFile
##         directory = os.path.dirname(wrapperName)

##         #  //
##         # // Make the JDL
##         #//
##         jdl = []
##         jdl.append("universe = globus\n")
##         jdl.append("globusscheduler = %s\n" % GlobusScheduler)
##         jdl.append("initialdir = %s\n" % directory)
##         jdl.append("Executable = %s\n" % wrapperName)
##         jdl.append("transfer_input_files = %s\n" % tarballName)
##         jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
##         jdl.append("should_transfer_files = YES\n")
##         jdl.append("when_to_transfer_output = ON_EXIT\n")
##         jdl.append("Output = %s-condor.out\n" % jobname)
##         jdl.append("Error = %s-condor.err\n" %  jobname)
##         jdl.append("Log = %s-condor.log\n" % jobname)
##         jdl.append("Queue\n")
        
        
##         handle = open(jdlFile, 'w')
##         handle.writelines(jdl)
##         handle.close()

##         #  //
##         # // Make the main wrapper
##         #//
        tarballBaseName = os.path.basename(tarballName)
        script = ["#!/bin/sh\n"]
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("cd $_CONDOR_SCRATCH_DIR\n")
        script.append("tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % tarballBaseName)
        script.append("cd %s\n" % jobname)
        script.append("./run.sh\n")
        script.append(
            "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
##    script.append("if [ -e $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml ]; then echo 1; else touch $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml; fi; ")
        
        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()
        return
    

    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        Build and run a condor_submit command

        """

        bossJobId=self.isBOSSDeclared()
        if bossJobId==None:
            self.declareToBOSS()
            bossJobId=self.isBOSSDeclared()
        #bossJobId=self.getIdFromFile(TarballDir, JobName)
        logging.debug( "BOSSCondorGSubmitter.doSubmit bossJobId = %s"%bossJobId)
        if bossJobId==0:
            return
        JobName=self.parameters['JobName']
        swversion=self.parameters['AppVersions'][0]  # only one sw version for now


        ## prepare scheduler related file 
        schedulercladfile = "%s/%s_scheduler.clad" %  (self.parameters['JobCacheArea'],self.parameters['JobName'])
        try:
           self.createJDL(schedulercladfile)
        except MissingConfigurationKey, ex:
           return 

        try:
          output=self.executeCommand("grid-proxy-info")
          output=output.split("timeleft :")[1].strip()
          if output=="0:00:00":
            #logging.info( "You need a grid-proxy-init")
            logging.error("grid-proxy-init expired")
            #sys.exit()
        except StandardError,ex:
          #print "You need a grid-proxy-init"
          logging.error("grid-proxy-init does not exist")
          sys.exit()
          
        bossSubmit = self.bossSubmitCommand[self.BossVersion](bossJobId)
        logging.debug("Scheduler clad file %s"%schedulercladfile)
        bossSubmit += " -schclassad %s"%schedulercladfile
    

        #  //
        # // Executing BOSS Submit command
        #//
        logging.debug( "BOSSCondorGSubmitter.doSubmit: %s"%bossSubmit)
        output = self.executeCommand(bossSubmit)
        logging.debug ("BOSSCondorGSubmitter.doSubmit: %s" %output)
        #os.remove(cladfile)



        
       ## command = "condor_submit %s" % wrapperScript
            
        ##print "BOSSCondorGSubmitter.doSubmit:", command
        ##output = self.executeCommand(command)
        ##print "BOSSCondorGSubmitter.doSubmit:", output
        return

    def createJDL(self, cladfilename):
        """
        _createJDL_
    
        create the scheduler JDL combining the user specified bit of the JDL
        """

        declareClad=open(cladfilename,"w")
                                                                                            
##         if not 'JDLRequirementsFile' in self.pluginConfig['OSG'].keys():
##           self.pluginConfig['OSG']['JDLRequirementsFile']=None

##         ## combine with the JDL provided by the user
##         user_requirements=""

##         if self.pluginConfig['OSG']['JDLRequirementsFile']!=None and self.pluginConfig['OSG']['JDLRequirementsFile']!='None':
##           if os.path.exists(self.pluginConfig['OSG']['JDLRequirementsFile']) :
##             logging.debug("createJDL: using JDLRequirementsFile "+self.pluginConfig['OSG']['JDLRequirementsFile'])
##             fileuserjdl=open(self.pluginConfig['OSG']['JDLRequirementsFile'],'r')
##             inlines=fileuserjdl.readlines()
##             for inline in inlines :
##               ## extract the Requirements specified by the user
##               if inline.find('Requirements') > -1 and inline.find('#') == -1 :
##                 UserReq = inline[ inline.find('=')+2 : inline.find(';') ]
##               ## write the other user defined JDL lines as they are
##               else :
##                 if inline.find('#') != 0 and len(inline) > 1 :
##                    declareClad.write(inline)
##             user_requirements=" %s && "%UserReq
##           else:
##             msg="JDLRequirementsFile File Not Found: %s"%self.pluginConfig['OSG']['JDLRequirementsFile']
##             logging.error(msg) 
##             raise InvalidFile(msg)
        if not 'GlobusScheduler' in self.pluginConfig['OSG'].keys():
            raise MissingConfigurationKey("GlobusSchedluer  not set")
        requirements='globusscheduler = %s\n'%self.pluginConfig['OSG']['GlobusScheduler']
        logging.debug('%s'%requirements)
        declareClad.write(requirements)
##         declareClad.write("VirtualOrganisation = \"cms\";\n")

##         ## change the RB according to user provided RB configuration files
##         if not 'GlobusScheduler' in self.pluginConfig['OSG'].keys():
##            self.pluginConfig['OSG']['GlobusScheduler']=None
##         if self.pluginConfig['OSG']['GlobusScheduler']!=None and self.pluginConfig['OSG']['GlobusScheduler']!='None':
##            if not os.path.exists(self.pluginConfig['OSG']['RBconfig']) :
##               msg="RBconfig File Not Found: %s"%self.pluginConfig['OSG']['RBconfig']
##               logging.error(msg)
              
##            declareClad.write('RBconfig = "'+self.pluginConfig['LCG']['RBconfig']+'";\n')

##         if not 'RBconfigVO' in self.pluginConfig['OSG'].keys():
##            self.pluginConfig['OSG']['RBconfigVO']=None
##         if self.pluginConfig['OSG']['RBconfigVO']!=None and self.pluginConfig['OSG']['RBconfigVO']!='None':
##            if not os.path.exists(self.pluginConfig['OSG']['RBconfigVO']) :
##               msg="RBconfigVO File Not Found: %s"%self.pluginConfig['OSG']['RBconfigVO']
##               logging.error(msg)
##               raise InvalidFile(msg)
##            declareClad.write('RBconfigVO = "'+self.pluginConfig['OSG']['RBconfigVO']+'";\n')

        declareClad.close()
        return


registerSubmitter(BOSSCondorGSubmitter,BOSSCondorGSubmitter.__name__ )
