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

        # do we want a default ?
        self.fileclass = None

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
        targetdir = None

        if targetdir == None:
            regExpParser = re.compile('/+castor/(.*)')
            if ( regExpParser.match(targetPFN) != None ):
                targetdir = os.path.dirname(targetPFN)

        if targetdir == None:
            regExpParser = re.compile('rfio:/+castor/(.*)')
            if ( regExpParser.match(targetPFN) != None ):
                targetdir = os.path.dirname('/castor/' + regExpParser.group(1))

        if targetdir == None:
            regExpParser = re.compile('rfio:.*path=/+castor/(.*)')
            if ( regExpParser.match(targetPFN) != None ):
                targetdir = os.path.dirname('/castor/' + regExpParser.group(1))

        # raise exception if we have no rule that can parse the target dir
        if targetdir == None:
            raise StageOutError("Cannot parse directory out of targetPFN")

        # remove multi-slashes from path
        while ( targetdir.find('//') > -1 ):
            targetdir = targetdir.replace('//','/')

        fileclass = self.fileclass

        # check to see if we need to change the file class
        regExpParser = re.compile('.*/castor/cern.ch/cms/store/data/[^/]+/[^/]+/RAW/')
        if ( regExpParser.match(targetdir) != None ):
            fileclass = 'cms_raw'

        # check if directory exists
        rfstatCmd = "rfstat %s 2> /dev/null | grep Protection" % targetdir
        print "Check dir existence : %s" % rfstatCmd
        try:
            rfstatExitCode, rfstatOutput = runCommandWithOutput(rfstatCmd)
        except Exception, ex:
            msg = "Error: Exception while invoking command:\n"
            msg += "%s\n" % rfstatCmd
            msg += "Exception: %s\n" % str(ex)
            msg += "Fatal error, abort stageout..."
            raise StageOutError(msg)

        # does not exist => create it
        if rfstatExitCode:
            if ( fileclass != None ):
                self.createDir(targetdir, '000')
                self.setFileClass(targetdir,fileclass)
                self.changeDirMode(targetdir, self.permissions)
            else:
                self.createDir(targetdir, self.permissions)
        else:
            # check if this is a directory
            regExpParser = re.compile('Protection.*: d')
            if ( regExpParser.match(rfstatOutput) == None):
                raise StageOutError("Output path is not a directory !")
            else:
                # check if directory is writable
                regExpParser = re.compile('Protection.*: d---------')
                if ( regExpParser.match(rfstatOutput) != None and fileclass != None ):
                    self.setFileClass(targetdir,fileclass)
                    self.makeDirWritable(targetdir)

        return


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an rfcp command

        """
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " '%s' " % sourcePFN
        result += " '%s' " % targetPFN
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
        
        result += "\nFILE_SIZE=`stat -c %s"
        result += " %s `;\n" % localPFN
        result += " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat '%s' | grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && [ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % remotePFN

        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-1

        """
        command = "stager_rm -M %s ; nsrm %s" % (pfnToRemove, pfnToRemove)
        execute(command)


    def createDir(self, directory, mode):
        """
        _createDir_

        Creates directory with no permissions

        """
        cmd = "nsmkdir -m %s -p %s" % (mode, directory)
        execute(cmd)

        return


    def setFileClass(self, directory, fileclass):
        """
        _createDir_

        Sets fileclass for specified directory

        """
        cmd = "nschclass %s %s" % (fileclass, directory)
        execute(cmd)

        return


    def changeDirMode(self, directory, mode):
        """
        _createDir_

        Sets mode for directory

        """
        cmd = "nschmod %s %s" % (mode, directory)
        execute(cmd)

        return


registerStageOutImpl("rfcp-CERN", RFCPCERNImpl)
