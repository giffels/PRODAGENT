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
from FwkJobRep.TaskState import TaskState, getTaskState
from FwkJobRep.MergeReports import updateReport

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery  import IMProvQuery

from MB.FileMetaBroker import FileMetaBroker
from MB.Persistency import load as loadMetabroker
from MB.transport.TransportFactory import getTransportFactory
from MB.MBException import MBException
from MB.TargetURL import transportTargetURL

_TransportFactory = None


from RunRes.RunResDBAccess import loadRunResDB, queryRunResDB


class StageOutFailure(Exception):
    pass



class StageOutSuccess(Exception):
    pass


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
            try:
                for template in self.templates:
                    try:
                        self.transferFile(fileInfo, template)
                    except StageOutFailure, failedInfo:
                        self.failed.append(failedInfo)
                        continue
            except StageOutSuccess, sucessInfo:
                self.succeeded.append(sucessInfo)
                continue

        for item in self.succeeded:
            print "Success:", item

        for item in self.failed:
            #  //
            # // Failures imply that input Node is failed 
            #//
            self.inputState._JobReport.status = "Failed"
            print "Failure:", item
            
            

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
        
        
        return

    def transferFile(self, fileInfo, template):
        """
        _transferFile_

        Perform a file transfer using the template instance provided

        """
        fmb = FileMetaBroker()
        for key, value in template.items():
            fmb[key] = value
        
        fmb['AbsName'] = fileInfo['PFN']
        fmb['TargetBaseName'] = fmb['BaseName']
        
        transporter = _TransportFactory[fmb['TransportMethod']]
        try:
            transporter.transportOut(fmb)
            targetURL = transportTargetURL(fmb)
            fileInfo['PFN'] = targetURL
        except MBException, ex:
            msg = "Transfer failed:\n"
            msg += str(ex)
            fileInfo['Failure'] = msg
            raise StageOutFailure, fileInfo

        raise StageOutSuccess, fileInfo
    
        
        

def stageOut():
    """
    _stageOut_

    Main function for this module. Loads data from the task
    and manages the stage out process for a single attempt

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()
    config = state._RunResDB.toDictionary()[state.taskAttrs['Name']]
    
    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    inputTask = config['StageOutParameters']['StageOutFor'][0]
    inputState = getTaskState(inputTask)
    
    
    manager = StageOutManager(state, inputState)
    manager()
    
    

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
    stageOut()
