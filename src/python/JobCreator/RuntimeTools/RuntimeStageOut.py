#!/usr/bin/env python
"""
_RuntimeStageOut_

Runtime script for managing a stage out job for files produced
by another node.

This script:

- Reads its own TaskState to get a MB.FileMetaBroker template for
the stage out, detailing the Xfer method, destination etc. It also gets
a list of workflow nodes to perform StageOut for.

- Reads the TaskState of each node that it has to stage out to get
  a list of files from the job report.

- For each file, it performs the stage out and updates the job report
  with the new PFN of the staged out file


"""

import os
import sys
import urlparse

from FwkJobRep.TaskState import TaskState, getTaskState
from FwkJobRep.MergeReports import updateReport


from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery  import IMProvQuery

from MB.FileMetaBroker import FileMetaBroker
from MB.Persistency import load as loadMetabroker
from MB.transport.TransportFactory import getTransportFactory
from MB.MBException import MBException
from MB.TargetURL import transportTargetURL
from MB.CreateDir import createDirectory

_TransportFactory = None


from RunRes.RunResDBAccess import loadRunResDB, queryRunResDB


class StageOutFailure(Exception):
    """
    _StageOutFailure_

    Self documenting Failure exception

    """
    def __init__(self, data):
        Exception.__init__(self)
        print "=========Stage Out Failure==========="
        for key, value in data.items():
            print key, value
        print "====================================="
        self.data = data
        

class StageOutSuccess(Exception):
    """
    _StageOutSuccess_

    Self documenting Failure exception

    """
    def __init__(self, data):
        Exception.__init__(self)
        print "File Staged Out: %s" % data['PFN']
        self.data = data

class StageOutManager:
    """
    _StageOutManager_

    Object for handling stage out of a set of files.

    """
    def __init__(self, stageOutTaskState, inputTaskState ):
        self.state = stageOutTaskState
        self.inputState = inputTaskState

        #  //
        # // load templates
        #//
        self.taskName = self.state.taskAttrs['Name']
        config = self.state._RunResDB.toDictionary()[self.taskName]
        templateFiles = config['StageOutParameters']['Templates']
        self.templates = loadTemplates(*templateFiles)
        
        
        self.toTransfer = []
        self.failed = []
        self.succeeded = []


        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None
        siteCfg = self.state.getSiteConfig()
        
            
        if siteCfg != None:
            try:
                self.tfc = siteCfg.trivialFileCatalog()
                msg = "Trivial File Catalog has been loaded:\n"
                msg += str(self.tfc)
                print msg
            except StandardError, ex:
                msg = "Unable to load Trivial File Catalog:\n"
                msg += "Local stage out will not be attempted\n"
                msg += str(ex)
                print msg
                self.tfc = None
            
            
                

    def __call__(self):
        """
        _operator()_

        Use call to invoke transfers

        """
        self.toTransfer = self.inputState.reportFiles()
        if len(self.toTransfer) == 0:
            msg =  "WARNING: No files found for stage out"
            msg += "Searched for files/catalogs in job report from:\n"
            msg += " %s\n" % self.inputState.jobReport
            msg += " %s\n" % self.inputState.runresdb
            msg += "The following catalogs were listed in the RunResDB:\n"
            for item in self.inputState.outputCatalogs():
                msg += " %s\n" % item
            msg += "\nThe following files were listed in the JobReport:\n"
            for item in self.inputState.reportFiles():
                msg += " %s\n" % item
            print msg
            return
        
        for fileInfo in self.toTransfer:
            localMatch = self.searchTFC(fileInfo['LFN'])
            if localMatch != None:
                #  //
                # // We have a local stage out match
                #//
                try:
                    #  //
                    # // Try local stage out
                    #//
                    self.localTransferFile(fileInfo, localMatch)
                except StageOutSuccess:
                    #  //
                    # // Local suceeded for this file
                    #//  log it and we are done
                    self.succeeded.append(fileInfo['LFN'])
                    continue
                except StageOutFailure:
                    #  //
                    # // No continue here: carry on to templates
                    #//  and record the failure
                    self.failed.append(fileInfo['LFN'])
            else:
                #  //
                # // No stage out locally counts as a potential 
                #//  failure, so record it.
                #  //If a template stage out works, it will be removed
                # // from the failure list
                #//
                self.failed.append(fileInfo['LFN'])
                
            try:
                #  //
                # // Still here, means using the reserve templates
                #//
                for template in self.templates:
                    try:
                        self.transferFile(fileInfo, template)
                    except StageOutFailure:
                        #  //
                        # // failed with this template, record
                        #//  failure and advance to next template
                        self.failed.append(fileInfo['LFN'])
                        continue
            except StageOutSuccess:
                #  //
                # // Stage out succeeded for some template, remove
                #//  lfn from failures and contiue to next file
                lfn = fileInfo['LFN'] 
                while lfn in self.failed:
                    self.failed.remove(lfn)
                self.succeeded.append(fileInfo['LFN'])
                continue
            
            
        exitCode = 0
        #  //
        # // Anything in failures means that the stage out overall
        #//  failed.
        for item in self.failed:
            #  //
            # // Failures imply that input task is failed 
            #//
            self.inputState._JobReport.status = "Failed"
            self.inputState._JobReport.exitCode = 60311
            exitCode = 60311
            
            

        #  //
        # // Now update the job report file for that task
        #//  with the new PFNS and propagate the changes to the
        #  //toplevel job report
        # //
        #//
        
        self.inputState.saveJobReport()
        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")
        
        
        updateReport(toplevelReport, self.inputState.getJobReport())
        print "Stage Out Complete: Exiting: %s " % exitCode
        
        
        return exitCode

    def transferFile(self, fileInfo, template):
        """
        _transferFile_

        Perform a file transfer using the template instance provided

        """
        fmb = FileMetaBroker()
        for key, value in template.items():
            fmb[key] = value
        
        fmb['AbsName'] = fileInfo['PFN']
        sePath = template['TargetPathName']
        lfn = fileInfo['LFN']
        absPath = "%s/%s" % (sePath, lfn)
        absPath = os.path.normpath(absPath)
        fmb['TargetAbsName'] = absPath
                              

        try:
            createDirectory(fmb)
        except StandardError, ex:
            #  //
            # // NOTE: Should probably throw StageOutFailure here
            #//  but it is a new thing so will give it some time.
            msg = "Error occurred when attempting to make directory:\n"
            msg += str(ex)
            print msg
        

        try:
            transporter = _TransportFactory[fmb['TransportMethod']]
            transporter.transportOut(fmb)
            targetURL = transportTargetURL(fmb)
            fileInfo['PFN'] = targetURL
        except MBException, ex:
            msg = "Transfer failed:\n"
            msg += str(ex)
            fileInfo['Failure'] = msg
            raise StageOutFailure, fileInfo

        raise StageOutSuccess, fileInfo

    def localTransferFile(self, fileInfo, template):
        """
        _localTransferFile_

        TFC matched LFN transfer
        
        """
        template['AbsName'] = fileInfo['PFN']


        try:
            createDirectory(template)
        except StandardError, ex:
            #  //
            # // NOTE: Should probably throw StageOutFailure here
            #//  but it is a new thing so will give it some time.
            msg = "Error occurred when attempting to make directory:\n"
            msg += str(ex)
            print msg
        
        try:
            transporter = _TransportFactory[template['TransportMethod']]
            transporter.transportOut(template)
            targetURL = transportTargetURL(template)
            fileInfo['PFN'] = targetURL
        except MBException, ex:
            msg = "Local Transfer failed:\n"
            msg += str(ex)
            fileInfo['Failure'] = msg
            raise StageOutFailure, fileInfo

        raise StageOutSuccess, fileInfo
        

    def searchTFC(self, lfn):
        """
        _searchTFC_

        Search the Trivial File Catalog for the lfn provided

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
        if self.tfc.preferredProtocol not in urlparse.uses_netloc:
            urlparse.uses_netloc.append(self.tfc.preferredProtocol)

        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            return

        msg = "LFN to PFN match made:\n"
        msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
        print msg

        template = FileMetaBroker()
        template['TransportMethod'] = self.tfc.preferredProtocol

        splitURL = urlparse.urlsplit(pfn)
        host = splitURL[1].strip()
        if len(host) > 0:
            template['TargetHostName'] = host
        template['TargetAbsName'] = splitURL[2]
        return template

def stageOut():
    """
    _stageOut_

    Main function for this module. Loads data from the task
    and manages the stage out process for a single attempt

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
        exitCode = 60311
        f = open("exit.status", 'w')
        f.write(str(exitCode))
        f.close()
        sys.exit(exitCode)

    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    inputTask = config['StageOutParameters']['StageOutFor'][0]
    inputState = getTaskState(inputTask)
    
    
    manager = StageOutManager(state, inputState)
    exitCode = manager()
    return exitCode
    

def loadTemplates(*templates):
    """
    _loadTemplates_

    for each file in the templates list, load the templates and
    convert them into FMB instances.

    Return the list of template FMBs

    """
    results = []
    for template in templates:
        try:
            improv = loadIMProvFile(template)
            query = IMProvQuery("MetaBroker")
            nodes = query(improv)
            for node in nodes:
                mbInstance = loadMetabroker(node, FileMetaBroker())
                results.append(mbInstance)
        except:
            continue
    return results






if __name__ == '__main__':
    _TransportFactory = getTransportFactory()
    exitCode = stageOut()
    f = open("exit.status", 'w')
    f.write(str(exitCode))
    f.close()
    sys.exit(exitCode)
    
