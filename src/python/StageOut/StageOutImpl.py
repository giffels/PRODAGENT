#!/usr/bin/env python
"""
_StageOutImpl_

Interface for Stage Out Plugins. All stage out implementations should
inherit this object and implement the methods accordingly

"""
import time
import os
from StageOut.Execute import execute
from StageOut.StageOutError import StageOutError

class StageOutImpl:
    """
    _StageOutImpl_

    Define the interface that needs to be implemented by stage out
    plugins

    Object attributes:

    - *numRetries* : Number of automated retry attempts if the command fails
                     default is 3 attempts
    - *retryPause* : Time in seconds to wait between retries.
                     default is 10 minutes
    """
    executeCommand = staticmethod(execute)

    def __init__(self):
        self.numRetries = 3
        self.retryPause = 600

    def createSourceName(self, protocol, pfn):
        """
        _createSource_

        construct a source URL/PFN for the pfn provided based on the
        protocol that can be passed to the stage out command that this
        implementation uses.

        """
        raise NotImplementedError, "StageOutImpl.createSourceName"


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        If a seperate step is required to create a directory in the
        SE for the stage out PFN provided, do that in this command.

        If no directory is required, do not implement this method
        """
        pass


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a shell command that will transfer the sourcePFN to the
        targetPFN using the options provided if necessary
        
        """
        raise NotImplementedError, "StageOutImpl.createStageOutCommand"


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        Construct and issue the command to remove the PFN provided as
        this impl requires.
        This will be used by the cleanup nodes in merge jobs that remove the
        intermediate files upon successful completion of the merge job

        """
        raise NotImplementedError, "StageOutImpl.removeFile"


    def createRemoveFileCommand(self, pfn):
        """
        return the command to delete a file after a failed copy
        """
        if pfn.startswith("/"):
            return "/bin/rm -f %s" % pfn
        else:
            return ""


    def __call__(self, protocol, inputPFN, targetPFN, options = None):
        """
        _Operator()_

        This operator does the actual stage out by invoking the overridden
        plugin methods of the derived object.


        """
        #  //
        # // Generate the source PFN from the plain PFN if needed
        #//
        sourcePFN = self.createSourceName(protocol, inputPFN)

        # destination may also need PFN changed
        # i.e. if we are staging in a file from an SE
        targetPFN = self.createSourceName(protocol, targetPFN)


        #  //
        # // Create the output directory if implemented
        #//
        self.createOutputDirectory(targetPFN)

        #  //
        # // Create the command to be used.
        #//
        command = self.createStageOutCommand(
            sourcePFN, targetPFN, options)

        #  //
        # // Run the command
        #//
        for retryCount in range(1, self.numRetries + 1):
            try:
                self.executeCommand(command)
                return
            except StageOutError, ex:
                msg = "Attempted stage out %s failed\n" % retryCount
                msg += "Automatically retrying in %s secs\n " % self.retryPause
                msg += "Error details:\n%s\n" % str(ex)
                print msg
                if retryCount == self.numRetries :
                    #  //
                    # // last retry, propagate exception
                    #//
                    raise ex
                time.sleep(self.retryPause)
                continue
                
                
        return

        
