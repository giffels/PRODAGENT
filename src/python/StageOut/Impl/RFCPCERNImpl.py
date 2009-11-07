#!/usr/bin/env python
"""
_RFCPCERNImpl_

Implementation of StageOutImpl interface for RFIO in Castor2
with specific code to set the RAW tape families for CERN

"""
import os
import re

from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl
from StageOut.StageOutError import StageOutError

from StageOut.Execute import execute
from StageOut.Execute import runCommandWithOutput

class RFCPCERNImpl(StageOutImpl):
    """
    _RFCPCERNImpl_
    
    """

    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.numRetries = 5
        self.retryPause = 300

        # permissions for target directory
        self.permissions = '775'


    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

         uses pfn

        """
        return "%s" % pfn


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """

        # check how the targetPFN looks like and parse out the target dir
        targetDir = None

        if targetDir == None:
            regExpParser = re.compile('/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetDir = os.path.dirname(targetPFN)

        if targetDir == None:
            regExpParser = re.compile('rfio:/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetDir = os.path.dirname('/castor/' + match.group(1))

        if targetDir == None:
            regExpParser = re.compile('rfio:.*path=/+castor/(.*)')
            match = regExpParser.match(targetPFN)
            if ( match != None ):
                targetDir = os.path.dirname('/castor/' + match.group(1))

        # raise exception if we have no rule that can parse the target dir
        if targetDir == None:
            raise StageOutError("Cannot parse directory out of targetPFN")

        # remove multi-slashes from path
        while ( targetDir.find('//') > -1 ):
            targetDir = targetDir.replace('//','/')

        print "DEBUG 111 before targetDir check"

        # check if targetDir exists
        targetDirCheck = "rfstat %s 2> /dev/null | grep Protection" % targetDir
        print "Check dir existence : %s" % targetDirCheck
        try:
            targetDirCheckExitCode, targetDirCheckOutput = runCommandWithOutput(targetDirCheck)
        except Exception, ex:
            msg = "Error: Exception while invoking command:\n"
            msg += "%s\n" % targetDirCheck
            msg += "Exception: %s\n" % str(ex)
            msg += "Fatal error, abort stageout..."
            raise StageOutError(msg)

        # does not exist => create it
        if targetDirCheckExitCode:

            print "DEBUG 222 have to create targetDir"

            # only use the fileclass code path if we run on t0export
            serviceClass = os.environ.get('STAGE_SVCCLASS', None)
            if serviceClass == 't0export':

                # determine file class from PFN
                fileclass = None

                print "DEBUG 333 checking PFN for fileclass override"

                # check for correct naming convention in PFN
                #regExpParser = re.compile('/castor/cern.ch/cms/store/data/[^/]+/[^/]+/[^/]+/')
                regExpParser = re.compile('/castor/cern.ch/cms/T0/hufnagel/recotest/store/data/([^/]+)/([^/]+)/([^/]+)/')
                match = regExpParser.match(targetDir)
                if ( match != None ):

                    # RAW data files use cms_raw, all others cms_production
                    if match.group(3) == 'RAW':
                        fileclass = 'cms_raw'
                    else:
                        fileclass = 'cms_production'

                    fileclassDir = '/castor/cern.ch/cms/T0/hufnagel/recotest/store/data/%s/%s/%s' % match.group(1,2,3)

                    print "DEBUG 444 want fileclass %s on %s" % (fileclass, fileclassDir)

                    # check fileclassDir existance
                    fileclassDirCheck = "rfstat %s 2> /dev/null | grep Protection" % fileclassDir
                    print "Check dir existence : %s" % fileclassDirCheck
                    try:
                        fileclassDirCheckExitCode, fileclassDirCheckOutput = runCommandWithOutput(fileclassDirCheck)
                    except Exception, ex:
                        msg = "Error: Exception while invoking command:\n"
                        msg += "%s\n" % rfstatCmd
                        msg += "Exception: %s\n" % str(ex)
                        msg += "Fatal error, abort stageout..."
                        raise StageOutError(msg)

                    # does not exist => create it
                    if fileclassDirCheckExitCode:
                        print "DEBUG 555 creating %s" % fileclassDir
                        self.createDir(fileclassDir, self.permissions)
                        if ( fileclass != None ):
                            print "DEBUG 666 setting fileclass %s" % fileclass
                            self.setFileClass(fileclassDir,fileclass)
                    else:
                        # check if this is a directory
                        regExpParser = re.compile('Protection.*: d')
                        if ( regExpParser.match(fileclassDirCheckOutput) == None):
                            raise StageOutError("Output path is not a directory !")

            # now create targetDir
            print "DEBUG 777 setting fileclass %s" % fileclass
            self.createDir(targetDir, self.permissions)

        else:

            # check if this is a directory
            regExpParser = re.compile('Protection.*: d')
            if ( regExpParser.match(targetDirCheckOutput) == None):
                raise StageOutError("Output path is not a directory !")

        return


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an rfcp command

        """
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " \"%s\" " % sourcePFN
        result += " \"%s\" " % targetPFN
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
        
        result += "\nFILE_SIZE=`rfstat \"%s\" | grep Size | cut -f2 -d:`\n" % localPFN
        result += " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat '%s' | grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && [ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % remotePFN

        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        """
        command = "stager_rm -M \"%s\" ; nsrm \"%s\"" % (pfnToRemove, pfnToRemove)
        execute(command)
        return

    def createDir(self, directory, mode):
        """
        _createDir_

        Creates directory with no permissions

        """
        cmd = "nsmkdir -m %s -p \"%s\"" % (mode, directory)
        execute(cmd)
        return


    def setFileClass(self, directory, fileclass):
        """
        _createDir_

        Sets fileclass for specified directory

        """
        cmd = "nschclass %s \"%s\"" % (fileclass, directory)
        execute(cmd)

        return


registerStageOutImpl("rfcp-CERN", RFCPCERNImpl)
