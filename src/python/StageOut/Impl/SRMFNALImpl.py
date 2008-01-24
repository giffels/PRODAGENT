#!/usr/bin/env python
"""
_SRMImpl_

Implementation of StageOutImpl interface for SRM

"""
import os
from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl


_CheckExitCodeOption = True



class SRMImpl(StageOutImpl):
    """
    _SRMImpl_

    Implement interface for srmcp command
    
    """
    
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        return "file:///%s" % pfn

    def createPnfsPath(self,pfn) :
        """
        _createPnfsPath_

        convert SRM pfn to PNDS pfn

        """
        return '/pnfs/cms/WAX' + pfn.split('=')[1]

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """

        # generate target pnfs path
        targetPnfsPath = self.createPnfsPath(targetPFN)
        
        result = "#!/bin/sh\n"
        result += "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -report=$REPORT_FILE -retry_num=0 "
        
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s \n" % targetPFN

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
            echo "srmcp exit status: $EXIT_STATUS"

            if [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero srmcp Exit status!!!"
               echo "Cleaning up failed file:"
               path=`echo %s | awk -F'=' '{print $2}'`
               path=`echo /pnfs/cms/WAX$path`
               /bin/rm -f $path
               exit 60311
            fi
        
            """ % targetPFN
        
        
        fileAbsPath = sourcePFN.replace("file://", "")
        result += "FILE_SIZE=`stat -c %s "
        result += " %s`\n" % fileAbsPath
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        metadataCheck = \
        """
        filesize() { cat "`dirname $1`/.(use)(2)(`basename $1`)'" | grep l= | sed -e's/.*;l=\([0-9]*\).*/\\1/'; }

        SRM_SIZE=`filesize %s`
        echo "SRM Size is $SRM_SIZE"
        if [[ $SRM_SIZE > 0 ]]; then
           if [[ $SRM_SIZE == $FILE_SIZE ]]; then
              exit 0
           else
              echo "Error: Size Mismatch between local and SE"
              echo "Cleaning up failed file:"
              /bin/rm -f %s
              exit 60311
           fi 
        fi
        echo "Cleaning up failed file:"
        /bin/rm -f %s 
        exit 60311

        """ % ( targetPnfsPath, targetPnfsPath, targetPnfsPath)
        result += metadataCheck
        
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "/bin/rm -f %s" % self.createPnfsPath(pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("srm", SRMImpl)
