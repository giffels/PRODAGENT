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

        # permission and umask for directory creation
        self.permissions = '775'
        self.umask = '002'

        # use castor support to preset adler32 checksums before transfer
        self.useChecksumForStageout = True


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
        targetDir = os.path.dirname(self.parseCastorPath(targetPFN))

        print "DEBUG 111 before targetDir check"

        # targetDir does not exist => create it
        if not self.checkDirExists(targetDir):

            print "DEBUG 222 have to create targetDir"

            # only use the fileclass code path if we run on t0export
            serviceClass = os.environ.get('STAGE_SVCCLASS', None)
            if serviceClass == 't0export':

                # determine file class from PFN
                fileclass = None

                print "DEBUG 333 checking PFN for fileclass override"

                # check for correct naming convention in PFN
                regExpParser = re.compile('/castor/cern.ch/cms/store/data/([^/]+)/([^/]+)/([^/]+)/')
                match = regExpParser.match(targetDir)
                if ( match != None ):

                    # RAW data files use cms_raw, all others cms_production
                    if match.group(3) == 'RAW':
                        fileclass = 'cms_raw'
                    else:
                        fileclass = 'cms_production'

                    fileclassDir = '/castor/cern.ch/cms/store/data/%s/%s/%s' % match.group(1,2,3)

                    print "DEBUG 444 want fileclass %s on %s" % (fileclass, fileclassDir)

                    # fileclassDir does not exist => create it
                    if not self.checkDirExists(fileclassDir):
                        print "DEBUG 555 creating %s" % fileclassDir
                        self.createDir(fileclassDir)
                        if ( fileclass != None ):
                            print "DEBUG 666 setting fileclass %s" % fileclass
                            self.setFileClass(fileclassDir,fileclass)

            # now create targetDir
            print "DEBUG 777 creating %s" % targetDir
            self.createDir(targetDir)

        return


    def createStageOutCommandWithChecksum(self, sourcePFN, targetPFN, options = None, checksums = None):
        """
        _createStageOutCommandWithChecksum_

        If adler32 checksum is available and this feature is supported,
        preset the checksum before the transfer to integrity check the transfer

        Otherwise use standard rfcp stageout

        """
        useChecksum = False

        if checksums != None and checksums.has_key('adler32') and not self.stageIn:

            print "DEBUG running castor version check"

            castorVersionCheck = "castor -v"
            try:
                castorVersionCheckExitCode, castorVersionCheckOutput = runCommandWithOutput(castorVersionCheck)
            except Exception, ex:
                msg = "Error: Exception while invoking command:\n"
                msg += "%s\n" % castorVersionCheck
                msg += "Exception: %s\n" % str(ex)
                msg += "Fatal error, abort stageout..."
                raise StageOutError(msg)

            if not castorVersionCheckExitCode:

                print "DEBUG castor version = %s" % castorVersionCheckOutput

                regExpParser = re.compile('([0-9]+).([0-9]+).([0-9]+)-([0-9]+)')
                match = regExpParser.match(castorVersionCheckOutput)

                if ( match != None ):

                    version1 = match.group(1)
                    version2 = match.group(2)
                    version3 = match.group(3)
                    subversion = match.group(4)

                    if ( ( version1 > 2 ) or \
                         ( version1 == 2 and version2 >1 ) or \
                         ( version1 == 2 and version2 == 1 and version3 > 8 ) or \
                         ( version1 == 2 and version2 == 1 and version3 == 8 and subversion >= 12 ) ):
                        useChecksum = True

        result = ""

        if useChecksum:

            print "DEBUG using adler 32 checksum %s for stageout" % checksums['adler32']

            targetFile = self.parseCastorPath(targetPFN)

            result += "nstouch %s\n" % targetFile
            result += "nssetchecksum -n adler32 -k %s %s\n" % (checksums['adler32'], targetFile)

        result += "rfcp "
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


    def checkDirExists(self, directory):
        """
        _checkDirExists_

        Check if directory exists (will fail if it exists as a file)

        """
        command = "rfstat %s 2> /dev/null | grep Protection" % directory
        print "Check dir existence : %s" % command
        try:
            exitCode, output = runCommandWithOutput(command)
        except Exception, ex:
            msg = "Error: Exception while invoking command:\n"
            msg += "%s\n" % command
            msg += "Exception: %s\n" % str(ex)
            msg += "Fatal error, abort stageout..."
            raise StageOutError(msg)

        if exitCode != 0:
            return False
        else:
            regExpParser = re.compile('^Protection[ ]+: d')
            if ( regExpParser.match(output) == None):
                raise StageOutError("Output path is not a directory !")
            else:
                return True


    def createDir(self, directory):
        """
        _createDir_

        Creates directory with correct permissions

        """
        command = "(umask %s ; nsmkdir -m %s -p \"%s\")" % (self.umask,
                                                            self.permissions,
                                                            directory)
        execute(command)
        return


    def setFileClass(self, directory, fileclass):
        """
        _setFileClass_

        Sets fileclass for specified directory

        """
        cmd = "nschclass %s %s" % (fileclass, directory)
        execute(cmd)
        return


    def parseCastorPath(self, complexCastorPath):
        """
        _parseCastorPath_

        Castor filenames can be full URLs
        with control statements for the rfcp

        Some other castor command line tools do
        not understand that syntax, so we need
        to retrieve the short version
        
        """
        simpleCastorPath = None

        if simpleCastorPath == None:
            regExpParser = re.compile('/+castor/cern.ch/(.*)')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/cern.ch/' + match.group(1)

        if simpleCastorPath == None:
            regExpParser = re.compile('rfio:.*/+castor/cern.ch/([^?]+).*')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/cern.ch/' + match.group(1)

        # if that does not work just use as-is
        if simpleCastorPath == None:
            simpleCastorPath = complexCastorPath

        # remove multi-slashes from path
        while ( simpleCastorPath.find('//') > -1 ):
            simpleCastorPath = simpleCastorPath.replace('//','/')

        return simpleCastorPath
        

registerStageOutImpl("rfcp-CERN", RFCPCERNImpl)
