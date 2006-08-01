#!/usr/bin/env python
"""
_RuntimeStageOut_

Runtime script for managing a stage out job for files produced
by another node.

This script:

- Reads its own TaskState to get the name of the task for which it is
 doing the stage out

- Reads the TaskState of the input node that it has to stage out to get
  a list of files from the job report.

- For each file, it performs the stage out and updates the job report
  with the new PFN of the staged out file


"""

import os
import sys


from StageOut.StageOutError import StageOutFailure
from StageOut.StageOutError import StageOutInitError

from StageOut.Registry import retrieveStageOutImpl

from FwkJobRep.TaskState import TaskState, getTaskState
from FwkJobRep.MergeReports import updateReport



class StageOutSuccess(Exception):
    """
    _StageOutSuccess_

    Exception used to escape stage out loop when stage out is successful
    """
    pass


class StageOutManager:
    """
    _StageOutManager_

    Object for handling stage out of a set of files.

    """
    def __init__(self, stageOutTaskState, inputTaskState ):
        self.state = stageOutTaskState
        self.inputState = inputTaskState
        self.inputReport = self.inputState.getJobReport()
     
        self.taskName = self.state.taskName()
        self.config = self.state.configurationDict()
        
        self.toTransfer = self.inputState.reportFiles()
        self.failed = {}
        self.succeeded = []
        #  //
        # // Are we overriding?
        #//
        self.override = False
        self.override = self.config['StageOutParameters'].has_key("Override")

        self.fallbacks = []
        
        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None
        self.siteCfg = self.state.getSiteConfig()
        

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        #//  then we need siteCfg otherwise we are dead.

        if (self.siteCfg == None) and (self.override == False):
            msg = "No Site Config Available and no Override:\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        


    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and TFC

        """
        
        implName = self.siteCfg.localStageOut.get("command", None)
        if implName == None:
            msg = "Unable to retrieve local stage out command\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg = "Local Stage Out Implementation to be used is:"
        msg += "%s\n" % implName

        seName = self.siteCfg.localStageOut.get("se-name", None)
        if seName == None:
            msg = "Unable to retrieve local stage out se-name\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg += "Local Stage Out SE Name to be used is %s\n" % seName
        catalog = self.siteCfg.localStageOut.get("catalog", None)
        if catalog == None:
            msg = "Unable to retrieve local stage out catalog\n"
            msg += "From site config file.\n"
            msg += "Unable to perform StageOut operation"
            raise StageOutInitError( msg)
        msg += "Local Stage Out Catalog to be used is %s\n" % catalog
        
        try:
            self.tfc = self.siteCfg.trivialFileCatalog()
            msg += "Trivial File Catalog has been loaded:\n"
            msg += str(self.tfc)
        except StandardError, ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Local stage out will not be attempted\n"
            msg += str(ex)
            raise StageOutInitError( msg )
        
        self.fallbacks = self.siteCfg.fallbackStageOut

        msg += "There are %s fallback stage out definitions.\n" % len(self.fallbacks)
        for item in self.fallbacks:
            msg += "Fallback to : %s using: %s \n" % (item['se-name'], item['command'])

        print msg
        return
        

    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        overrideConf = self.config['StageOutParameters']['Override']
        overrideParams = {
            "command" : None,
            "option" : None,
            "se-name" : None,
            "lfn-prefix" : None,
            }

        try:
            overrideParams['command'] = overrideConf['command'][-1]
            overrideParams['se-name'] = overrideConf['se-name'][-1]
            overrideParams['lfn-prefix'] = overrideConf['lfn-prefix'][-1]
        except StandardError, ex:
            msg = "Unable to extract Override parameters from config:\n"
            msg += str(self.config['StageOutParameters'])
            raise StageOutInitError(msg)
        if overrideConf.has_key('option'):
            overrideParams['option'] = overrideConf['option'][-1]
        
        msg = "=======StageOut Override Initialised:================\n"
        for key, val in overrideParams.items():
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        print msg
        self.fallbacks = []
        self.fallbacks.append(overrideParams)
        return 
        
        
    def __call__(self):
        """
        _operator()_

        Use call to invoke transfers

        """
        #  //
        # // First check to see if we are using override
        #//  if so, invoke for all files and exit
        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()            
            
        for fileToStage in self.toTransfer:
            try:
                print "==>Working on file: %s" % fileToStage['LFN']
                if fileToStage['GUID'] != None:
                    fileToStage['LFN'] = os.path.join(
                        os.path.dirname(fileToStage['LFN']),
                        "%s.root" % fileToStage['GUID']
                        )
                    print "==> GUID inserted into LFN: %s" % fileToStage['LFN']
                lfn = fileToStage['LFN']
                
                #  //
                # // No override => use local-stage-out from site conf
                #//  invoke for all files and check failures/successes
                if not self.override:
                    print "===> Attempting Local Stage Out."
                    try:
                        pfn = self.localStageOut(lfn, fileToStage['PFN'])
                        fileToStage['PFN'] = pfn
                        fileToStage['SEName'] = self.siteCfg.localStageOut['se-name']
                        raise StageOutSuccess
                    except StageOutFailure, ex:
                        if not self.failed.has_key(lfn):
                            self.failed[lfn] = []
                        self.failed[lfn].append(ex)
                    
                #  //
                # // Still here => failure, start using the fallback stage outs
                #//  If override is set, then that will be the only fallback available
                print "===> Attempting %s Fallback Stage Outs" % len(self.fallbacks)
                for fallback in self.fallbacks:
                    try:
                        pfn = self.fallbackStageOut(lfn, fileToStage['PFN'],
                                                    fallback)
                        fileToStage['PFN'] = pfn
                        fileToStage['SEName'] = fallback['se-name']
                        if self.failed.has_key(lfn):
                            del self.failed[lfn]
                        raise StageOutSuccess
                    except StageOutFailure, ex:
                        if not self.failed.has_key(lfn):
                            self.failed[lfn] = []
                        self.failed[lfn].append(ex)
                        continue
            except StageOutSuccess:
                msg = "===> Stage Out Successful:\n"
                msg += "====> LFN: %s\n" % fileToStage['LFN']
                msg += "====> PFN: %s\n" % fileToStage['PFN']
                msg += "====> SE:  %s\n" % fileToStage['SEName']
                print msg
                continue

        #  //
        # // Check for failures and update reports if there are any.
        #//
        exitCode = 0
        if len(self.failed.keys()) > 0:
            for lfn in self.failed.keys():
                for err in self.failed[lfn]:
                    self.reportStageOutFailure(err)
            self.inputReport.status = "Failed"
            self.inputReport.exitCode = 60312
            exitCode = 60312
        self.inputState.saveJobReport()

        return exitCode

    def fallbackStageOut(self, lfn, localPfn, fbParams):
        """
        _fallbackStageOut_

        Given the lfn and parameters for a fallback stage out, invoke it

        parameters should contain:

        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        se-name - the Name of the SE to which the file is being xferred
        
        """
        pfn = "%s%s" % (fbParams['lfn-prefix'], lfn)

        try:
            impl = retrieveStageOutImpl(fbParams['command'])
        except Exception, ex:
            msg = "Unable to retrieve impl for fallback stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: "
            msg += "%s\n" % fbParams['command']
            raise StageOutFailure(msg, Command = fbParams['command'],
                                  LFN = lfn, ExceptionDetail = str(ex))
        
        try:
            impl(fbParams['command'], localPfn, pfn, options)
        except Exception, ex:
            msg = "Failure for fallback stage out:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command = command, 
                                  LFN = lfn, InputPFN = localPfn,
                                  TargetPFN = pfn)
            
        return pfn
        
    def localStageOut(self, lfn, localPfn):
        """
        _localStageOut_

        Given the lfn and local stage out params, invoke the local stage out

        """
        seName = self.siteCfg.localStageOut['se-name']
        command = self.siteCfg.localStageOut['command']
        options = self.siteCfg.localStageOut.get('options', None)
        pfn = self.searchTFC(lfn)
        protocol = self.tfc.preferredProtocol
        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN = lfn, TFC = str(self.tfc))

        
        try:
            impl = retrieveStageOutImpl(command)
        except Exception, ex:
            msg = "Unable to retrieve impl for local stage out:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command = command,
                                  LFN = lfn, ExceptionDetail = str(ex))
        
        try:
            impl(protocol, localPfn, pfn, options)
        except Exception, ex:
            msg = "Failure for local stage out:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command = command, Protocol = protocol,
                                  LFN = lfn, InputPFN = localPfn,
                                  TargetPFN = pfn)
        
        return pfn


    
    def reportStageOutFailure(self, stageOutExcep):
        """
        _reportStageOutFailure_

        When a stage out failure occurs, report it to the input
        framework job report.

        - *stageOutExcep* : Instance of on of the StageOutError derived classes
        
        """
        errStatus = stageOutExcep.data["ErrorCode"]
        errType = stageOutExcep.data["ErrorType"]
        desc = stageOutExcep.message

        errReport = self.inputReport.addError(errStatus, errType)
        errReport['Description'] = desc
        return


        
      
    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided,
        if a match is made, return the matched PFN

        """
        if self.tfc == None:
            msg = "Trivial File Catalog not available to match LFN:\n"
            msg += lfn
            print msg
            return None
        if self.tfc.preferredProtocol == None:
            msg = "Trivial File Catalog does not have a preferred protocol\n"
            msg += "which prevents local stage out for:\n"
            msg += lfn
            print msg
            return None
        
        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return None

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        print msg

        return pfn

def stageOut():
    """
    _stageOut_

    Main function for this module. Loads data from the task
    and manages the stage out process for a single attempt

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()
    config = state.configurationDict()

    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    inputTask = config['StageOutParameters']['StageOutFor'][0]
    inputState = getTaskState(inputTask)
    
    try:
        manager = StageOutManager(state, inputState)
        exitCode = manager()
    except StageOutInitError, ex:
        exitCode = ex.data['ErrorCode']
        inputReport = inputState.getJobReport()
        errRep = inputReport.addError(
            ex.data['ErrorCode'], ex.data['ErrorType'])
        errRep['Description'] = ex.message
        inputReport.status = "Failed"
        inputReport.exitCode = ex.data['ErrorCode']
        inputState.saveJobReport()
        
    #  //
    # // Update primary job report
    #//
    toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")


    updateReport(toplevelReport, inputState.getJobReport())
    print "Stage Out Complete: Exiting: %s " % exitCode
    return exitCode
    







if __name__ == '__main__':
    import StageOut.Impl
    exitCode = stageOut()
    f = open("exit.status", 'w')
    f.write(str(exitCode))
    f.close()
    sys.exit(exitCode)
    
