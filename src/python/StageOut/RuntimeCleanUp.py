#!/usr/bin/env python
"""
_RuntimeCleanUp_

Runtime binary file for CleanUp type nodes

"""
import sys
import os
from FwkJobRep.TaskState import TaskState, getTaskState

import StageOut.Impl
from StageOut.Registry import retrieveStageOutImpl

class CleanUpSuccess(Exception):
    """
    _CleanUpSuccess_

    """
    def __init__(self, lfn, pfn):
        Exception.__init__(self, "CleanUpSuccess")
        msg = "Succesful Cleanup of LFN:\n%s\n" % lfn
        msg += "  PFN: %s\n" % pfn
        print msg

class CleanUpFailure(Exception):
    """
    _CleanUpFailure_

    """
    def __init__(self, lfn, **details):
        Exception.__init__(self, "CleanUpFailure")
        self.lfn = lfn
        msg = "================CleanUp Failure========================\n"
        msg += " Failed to clean up file:\n"
        msg += " %s\n" % lfn
        msg += " Details:\n"
        for key, val in details.items():
            msg += "  %s: %s\n" % (key, val)

        print msg
        
class SkippedFileFilter:
    def __init__(self, skippedFiles):
        self.skipped = [ i['Lfn'] for i in skippedFiles ] 

    def __call__(self, filedata):
        return filedata['LFN'] not in self.skipped

class CleanUpManager:
    """
    _CleanUpManager_

    Object that is invoked to do the cleanup operation

    """
    def __init__(self, cleanUpTaskState, inputTaskState = None ):
        self.state = cleanUpTaskState
        self.inputState = inputTaskState
        #  //
        # // load templates
        #//
        self.taskName = self.state.taskAttrs['Name']
        self.config = self.state._RunResDB.toDictionary()[self.taskName]

        if self.inputState != None:
            self.cleanUpInput()
        else:
            self.cleanUpFileList()

        self.setupCleanup()
        

    def cleanUpInput(self):
        """
        _cleanUpInput_

        This cleanup node is for cleaning after an input job
        Eg post merge cleanup
        
        """
        msg = "Cleaning up input files for job: "
        msg += self.inputState.taskAttrs['Name']
        print msg
        self.inputState.loadJobReport()
        inputReport = self.inputState.getJobReport()
        
        inputFileDetails = filter(
            SkippedFileFilter(inputReport.skippedFiles),
            inputReport.inputFiles)

        
        self.inputFiles = [ i['LFN'] for i in inputFileDetails ] 


    def cleanUpFileList(self):
        """
        _cleanUpFileList_

        List of LFNs is provided in the RunResDB for this node

        """
        msg = "Cleaning up list of files:\n"

        lfnList = self.config.get("RemoveLFN", [])
        if len(lfnList) == 0:
            msg += "No Files Found in Configuration!!!"

        for lfn in lfnList:
            msg += " Removing: %s\n" % lfn

        print msg

        self.inputFiles = lfnList
        return
        
        
        

    def setupCleanup(self):
        """
        _setupCleanup_

        Setup for cleanup operation: Read in siteconf and TFC

        """
        
        self.success = []
        self.failed = []
        
        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None
        siteCfg = self.state.getSiteConfig()
        
            
        if siteCfg == None:
            msg = "No Site Config Available:\n"
            msg += "Unable to perform CleanUp operation"
            raise RuntimeError, msg
        
        try:
            self.tfc = siteCfg.trivialFileCatalog()
            msg = "Trivial File Catalog has been loaded:\n"
            msg += str(self.tfc)
            print msg
        except StandardError, ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Clean Up will not be attempted\n"
            msg += str(ex)
            raise RuntimeError, msg

        
        
        #  //
        # // Lookup StageOut Impl name that will be used to generate
        #//  cleanup
        self.implName = siteCfg.localStageOut.get("command", None)
        if self.implName == None:
            msg = "Unable to retrieve local stage out command\n"
            msg += "From site config file.\n"
            msg += "Unable to perform CleanUp operation"
            raise RuntimeError, msg
        msg = "Stage Out Implementation to be used for cleanup is:"
        msg += "%s" % self.implName
        
        

    def __call__(self):
        """
        _operator()_

        Invoke cleanup operation

        """
        for deleteFile in self.inputFiles:
           
            try:
                print "Deleting File: %s" % deleteFile
                self.invokeCleanUp(deleteFile)
                self.success.append(deleteFile)
            except CleanUpFailure, ex:
                self.failed.append(deleteFile)


                

        status = 0
        msg = "The following LFNs have been cleaned up successfully:\n"
        for lfn in self.success:
            msg += "  %s\n" % lfn
        
        if len(self.failed) > 0:
            msg += "The following LFNs could not be removed:\n"
            for lfn in self.failed:
                msg += "  FAILED:%s\n" % lfn
            status = 60312

        msg += "Exit Status for this task is: %s\n" % status
        print msg
        return status

        
    def invokeCleanUp(self, lfn):
        """
        _invokeCleanUp_

        Instantiate the StageOut impl, map the LFN to PFN using the TFC
        and invoke the CleanUp on that PFN

        """
        #  //
        # // Load Impl
        #//
        try:
            implInstance = retrieveStageOutImpl(self.implName)
        except Exception, ex:
            msg = "Error retrieving Stage Out Impl for name: "
            msg += "%s\n" % self.implName
            msg += str(ex)
            raise CleanUpFailure(lfn, 
                                 ImplName = self.implName,
                                 Message = msg)
        
        #  //
        # // Match LFN
        #//
        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            raise CleanUpFailure(lfn, TFC = str(self.tfc),
                                 ImplName = self.implName,
                                 Message = msg,
                                 TFCProtocol = self.tfc.preferredProtocol)
        
        #  //
        # //  Invoke StageOut Impl removeFile method
        #//
        try:
            implInstance.removeFile(pfn)
        except Exception, ex:
            msg = "Error performing Cleanup command for impl "
            msg += "%s\n" % self.implName
            msg += "On PFN: %s\n" % pfn
            msg += str(ex)
            raise CleanUpFailure(lfn, TFC = str(self.tfc),
                                 ImplName = self.implName,
                                 PFN = pfn,
                                 Message = msg,
                                 TFCProtocol = self.tfc.preferredProtocol)
        
        


def cleanUp():
    """
    _cleanUp_

    Main program

    """
    
    state = TaskState(os.getcwd())
    state.loadRunResDB()
    try:
        config = state._RunResDB.toDictionary()[state.taskAttrs['Name']]
    except StandardError, ex:
        msg = "Unable to load details from task directory:\n"
        msg += "Error reading RunResDB XML file:\n"
        msg += "%s\n" % state.runresdb 
        msg += "and extracting details for task in: %s\n" % os.getcwd()
        print msg
        exitCode = 60312
        f = open("exit.status", 'w')
        f.write(str(exitCode))
        f.close()
        sys.exit(exitCode)

    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    inputTask = config['CleanUpParameters']['CleanUpFor'][0]
    inputState = getTaskState(inputTask)
    
    
    manager = CleanUpManager(state, inputState)
    exitCode = manager()
    return exitCode


if __name__ == '__main__':
    print "RuntimeCleanUp invoked..."
    exitCode = cleanUp()
    f = open("exit.status", 'w')
    f.write(str(exitCode))
    f.close()
    sys.exit(exitCode)
    
