#!/usr/bin/env python
"""
_LCGSubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id: LCGSubmitter.py,v 1.6 2006/05/08 08:51:18 bacchi Exp $"

#  //
# // Configuration variables for this submitter
#//
#bossJobType = ""  # some predetermined type value from boss install here
bossScheduler = "edg"

#  //
# // End of Config variables.
#//

import os
import sys
import logging
import exceptions
from MCPayloads.JobSpec import JobSpec
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException

class InvalidUserJDLFile(exceptions.Exception):
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
        
        self.parameters['Scheduler']="edg"
        self.bossSubmitCommand={"v3":self.BOSS3submit,"v4":self.BOSS4submit}


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
            return
        JobName=self.parameters['JobName']
        swversion=self.parameters['AppVersions'][0]  # only one sw version for now


        ## prepare scheduler related file 
        schedulercladfile = "%s/%s_scheduler.clad" %  (self.parameters['JobCacheArea'],self.parameters['JobName'])
        try:
           self.createJDL(schedulercladfile,swversion)
        except InvalidUserJDLFile, ex:
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
        bossSubmit += " -schclassad %s"%schedulercladfile

        #  //
        # // Executing BOSS Submit command
        #//
        logging.debug( "LCGSubmitter.doSubmit:", bossSubmit)
        output = self.executeCommand(bossSubmit)
        logging.debug ("LCGSubmitter.doSubmit: %s" % output)
        #os.remove(cladfile)
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
            user_requirements=" %s && "%UserReq
          else:
            msg="JDLRequirementsFile File Not Found: %s"%self.pluginConfig['LCG']['JDLRequirementsFile']
            logging.error(msg) 
            raise InvalidUserJDLFile(msg)

        requirements='Requirements = %s Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment);\n'%(user_requirements,swversion)
        logging.debug('%s'%requirements)
        declareClad.write(requirements)
        declareClad.write("VirtualOrganisation = \"cms\";\n")
        declareClad.close()
        return


registerSubmitter(LCGSubmitter, "lcg")
