#!/usr/bin/env python
"""
_SRMImpl_

Implementation of StageOutImpl interface for SRM

"""
import os
from StageOut.Registry import registerStageOutImpl
from StageOut.StageOutImpl import StageOutImpl






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

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """

        result = "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -report=$REPORT_FILE "
        
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s \n" % targetPFN

        result += """
        EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
        echo "srmcp exit status: $EXIT_STATUS"
        if (( $EXIT_STATUS != 0 )); then
            echo "Non-zero srmcp Exit status!!!"
            exit $EXIT_STATUS
        fi
        
        """
        
        
        fileAbsPath = sourcePFN.replace("file://", "")
        result += "FILE_SIZE=`stat -c %s "
        result += " %s`\n" % fileAbsPath
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        metadataCheck = \
        """
        for ((a=1; a <= 10 ; a++))
        do
           SRM_SIZE=`srm-get-metadata -retry_num=0 %s 2>/dev/null | grep 'size :[0-9]' | cut -f2 -d":"`
           echo "SRM Size is $SRM_SIZE"
           if (( $SRM_SIZE > 0 )); then
              if (( $SRM_SIZE == $FILE_SIZE )); then
                 exit 0
              else
                 echo "Error: Size Mismatch between local and SE"
                 exit 60311
              fi 
           else
              sleep 2
           fi
        done
        exit 60311

        """ % targetPFN
        result += metadataCheck
        
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "srm-advisory-delete %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("srm", SRMImpl)
