#!/usr/bin/env python
"""
python

Glite Collection class

"""

__revision__ = "$Id: BlGLiteBulkSubmitter.py,v 1.1 2008/05/15 15:09:55 gcodispo Exp $"
__version__ = "$Revision: 1.1 $"

import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BossLiteBulkInterface import BossLiteBulkInterface
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ProdAgentException import ProdAgentException

import exceptions
class InvalidFile(exceptions.Exception):
    """
    local exception
    """
    def __init__(self, msg):
        args = "%s\n" % msg
        exceptions.Exception.__init__(self, args)


class BlGLiteBulkSubmitter(BossLiteBulkInterface):
    """

    Base class for GLITE bulk submission should not be used
    directly but one of its inherited classes.
      
    """

    scheduler  = "SchedulerGLiteAPI"


    def getSchedulerConfig(self) :
        """
        _getSchedulerConfig_

        retrieve configuration info for the BossLite scheduler
        """

        if not 'WMSconfig' in self.pluginConfig['GLITE'].keys() or \
               self.pluginConfig['GLITE']['WMSconfig'] is None \
               or self.pluginConfig['GLITE']['WMSconfig'] == 'None' :

            schedulerConfig = ''

        elif os.path.exists( self.pluginConfig['GLITE']['WMSconfig'] ) :
            schedulerConfig = self.pluginConfig['GLITE']['WMSconfig']
        else :
            schedulerConfig = ''
            logging.error( "WMSconfig File Not Found: %s" % \
                           self.pluginConfig['GLITE']['WMSconfig'] )

        return schedulerConfig


    def createSchedulerAttributes(self, jobType):
        """
        _createSchedulerAttributes_
    
        create the scheduler JDL combining the user specified bit of the JDL
        """

        submissionAttrs = ''

        userJDLRequirementsFile = self.getUserJDL(jobType)

        #  //
        # // combine with the JDL provided by the user
        #//
        userRequirements = ""

        if userJDLRequirementsFile != "None":

            if os.path.exists(userJDLRequirementsFile) :
                userReq = None
                logging.debug( "createJDL: using JDLRequirementsFile " \
                               + userJDLRequirementsFile )
                fileuserjdl = open(userJDLRequirementsFile, 'r')
                inlines = fileuserjdl.readlines()
                for inline in inlines :
                    ## extract the Requirements specified by the user
                    if inline.find('Requirements') > -1 \
                           and inline.find('#') == -1:
                        userReq = \
                                inline[ inline.find('=')+2 : inline.find(';') ]
                    ## write the other user defined JDL lines as they are
                    else :
                        if inline.find('#') != 0 and len(inline) > 1 :
                            submissionAttrs += inline
                if userReq != None :
                    userRequirements = " %s " % userReq
            else:
                msg = "JDLRequirementsFile File Not Found: %s" \
                      % userJDLRequirementsFile
                logging.error(msg)
                raise InvalidFile(msg)

        anyMatchrequirements = self.getSiteRequirements()

        #  //
        # // CMSSW arch
        #//
        swarch = None
        creatorPluginConfig = loadPluginConfig("JobCreator",
                                                  "Creator")
        if creatorPluginConfig['SoftwareSetup'].has_key('ScramArch'):
            if creatorPluginConfig['SoftwareSetup']['ScramArch'].find("slc4") >= 0:
                swarch = creatorPluginConfig['SoftwareSetup']['ScramArch']

        if swarch:
            archrequirement = " Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) " % swarch
        else:
            archrequirement = ""

        #  //
        # // software version requirements
        #//
        if jobType in ("CleanUp", "LogCollect"):
            swClause = ""
        else:
            if len(self.applicationVersions) > 0:
                swClause = " ("
                for swVersion in self.applicationVersions:
                    swClause += "Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment) " % swVersion
                    if swVersion != self.applicationVersions[-1]:
                    # Not last element, need logical AND
                        swClause += " && "
                swClause += ")"
            else:
                raise ProdAgentException("No CMSSW version found!")


        #  //
        # // building jdl
        #//

        requirements = "%s"% userRequirements
        if swClause != "":
            if requirements != "":
                requirements += " && "
            requirements  += " %s " % swClause
        if archrequirement != "" :
            if requirements != "":
                requirements += " && "
            requirements += " %s " % archrequirement
        if anyMatchrequirements != "" :
            if requirements != "":
                requirements += " && "
            requirements += " %s " % anyMatchrequirements
        # add requirement for CE in Production state
        requirements += " && other.GlueCEStateStatus == \"Production\" " 

        if requirements != "":
            requirements = "Requirements = %s ;\n" % requirements
            logging.info('%s' % requirements)
            submissionAttrs += requirements
#        declareClad.write("Environment = {\"PRODAGENT_DASHBOARD_ID=%s\"};\n"%self.parameters['DashboardID'])

        submissionAttrs += "VirtualOrganisation = \"cms\";\n"

        return submissionAttrs


    def getSiteRequirements(self):
        """
        # // white list for anymatch clause
        """
        anyMatchrequirements = ""
        if len(self.whitelist) > 0:
            anyMatchrequirements = " ("
            sitelist = ""
            for i in self.whitelist:
                logging.debug("Whitelist element %s" % i)
                sitelist += "Member(\"%s\" , other.GlueCESEBindGroupSEUniqueID)" % i + " || "
                #sitelist+="target.GlueSEUniqueID==\"%s\""%i+" || "
            sitelist = sitelist[:len(sitelist)-4]
            anyMatchrequirements += sitelist + ")"
        return anyMatchrequirements




    def getUserJDL(self, jobType):
        """
        _getUserJDL_
 
        get the user defined JDL in the Submitter config file according to the job type:
          o Merge type: look for MergeJDLRequirementsFile first, then default to JDLRequirementsFile
          o Porcessing type: look for JDLRequirementsFile 
        """
        userJDLRequirementsFile = "None"
        #
        #  For Merge jobs use Merge JDLRequirementsFile if it's configured
        #
        if jobType in ("Merge", "CleanUp", "LogCollect"):
            if 'MergeJDLRequirementsFile' in self.pluginConfig['GLITE'].keys():
                userJDLRequirementsFile = \
                         self.pluginConfig['GLITE']['MergeJDLRequirementsFile']
                return userJDLRequirementsFile
        #
        #  Use JDLRequirementsFile if it's configured
        #
        if 'JDLRequirementsFile' in self.pluginConfig['GLITE'].keys():
            userJDLRequirementsFile = \
                              self.pluginConfig['GLITE']['JDLRequirementsFile']
            return userJDLRequirementsFile

        return userJDLRequirementsFile

registerSubmitter(BlGLiteBulkSubmitter, BlGLiteBulkSubmitter.__name__)

