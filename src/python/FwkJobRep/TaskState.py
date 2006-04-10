#!/usr/bin/env python
"""
_TaskState_

Runtime interface for reading in the state of a Task by reading the
RunResDB.xml and FrameworkJobReport.xml files and providing an
API to access the contents of them.

The object is instantiated with a directory that contains the task.


"""

__version__ = "$Revision: 1.11 $"
__revision__ = "$Id: TaskState.py,v 1.11 2006/03/30 22:07:42 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os
import popen2

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from RunRes.RunResComponent import RunResComponent
from RunRes.RunResDBAccess import loadRunResDB

from FwkJobRep.ReportParser import readJobReport
from FwkJobRep.CatalogParser import readCatalog


def getTaskState(taskName):
    """
    _getTaskState_

    Find a task with the name provided in a job and, if it exists,
    instantiate a TaskState object for that Task.

    This method uses the RunResDB to look up the task location within
    the job and instantiates a TaskState for that task

    If the task is not found, None is returned

    """
    runresdb = os.environ.get("RUNRESDB_URL", None)
    if runresdb == None:
        return None
    try:
        rrdb = loadRunResDB(runresdb)
    except:
        return None
    query = "/RunResDB/%s/Directory[text()]" % taskName
    result =  rrdb.query(query)
    if len(result) == 0:
        return None
    result = result[0]
    dirname = os.path.join(os.environ['PRODAGENT_JOB_DIR'], result)
    
    taskState = TaskState(dirname)
    taskState.loadJobReport()
    taskState.loadRunResDB()
    return taskState


class TaskState:
    """
    _TaskState_

    API object for extracting information from a CMSSW Task from the
    components of that task including the RunResDB and FrameworkJobReport
    

    """
    def __init__(self, taskDirectory):
        self.dir = taskDirectory
        self.jobReport = os.path.join(self.dir, "FrameworkJobReport.xml")
        self.runresdb = os.path.join(self.dir, "RunResDB.xml")

        self.taskAttrs = {}
        self.taskAttrs.setdefault("Name", None)
        self.taskAttrs.setdefault("CfgFile", None)
        self.taskAttrs.setdefault("PyCfgFile", None)
        self.taskAttrs.setdefault("WorkflowSpecID", None)
        self.taskAttrs.setdefault("JobSpecID", None)
        
        
        self._RunResDB = None
        self._JobReport = None
        self.runresLoaded = False
        self.jobReportLoaded = False
        
        


    def loadRunResDB(self):
        """
        _loadRunResDB_

        If the RunResDB file exists, load it

        """
        if not os.path.exists(self.runresdb):
            return
        improvNode = loadIMProvFile(self.runresdb)
        self._RunResDB = RunResComponent()
        self._RunResDB.children = improvNode.children
        self.runresLoaded = True

        dbDict = self._RunResDB.toDictionary()
        self.taskAttrs['Name'] = dbDict.keys()[0]
        
        self.taskAttrs['WorkflowSpecID'] = \
                 dbDict[self.taskAttrs['Name']]['WorkflowSpecID'][0]
        self.taskAttrs['JobSpecID'] = \
                 dbDict[self.taskAttrs['Name']]['JobSpecID'][0]
        
        return
        

    def getExitStatus(self):
        """
        _getExitStatus_

        If the task dir contains a file named exit.status, it will be
        read and converted into an integer and returned

        If the file does not exist, or cannot be parsed into an integer,
        None will be returned

        """
        exitFile = os.path.join(self.dir, "exit.status")
        if not os.path.exists(exitFile):
            return None
        content = file(exitFile).read()
        content = content.strip()
        try:
            exitCode = int(content)
            return exitCode
        except:
            return None
        
        

    def loadJobReport(self):
        """
        _loadJobReport_

        Extract the JobReport from the job report file if it exists

        """
        if not os.path.exists(self.jobReport):
            return
        
        jobReport = readJobReport(self.jobReport)[0]
        self._JobReport = jobReport
        self.jobReportLoaded = True

        #  //
        # // Convert PFNs to absolute paths if they exist in this
        #//  directory
        for fileInfo in self._JobReport.files:
            pfn = fileInfo['PFN']
            if pfn.startswith("file:"):
                pfn = pfn.replace("file:", "")
            
            pfnPath = os.path.join(self.dir, pfn)
            if not os.path.exists(pfnPath):
                continue
            fileInfo['PFN'] = pfnPath
        return
    
        
    def getJobReport(self):
        """
        _getJobReport_

        Return a reference to the FkwJobReport object so that it can be
        manipulated

        """
        return self._JobReport
    
    def saveJobReport(self):
        """
        _saveJobReport_

        After modifying the JobReport in memory, commit the changes back to
        the JobReport file

        """
        self._JobReport.write(os.path.join(self.dir,"FrameworkJobReport.xml"))
        return
    
    

    def outputDatasets(self):
        """
        _outputDatasets_

        Retrieve a list of output datasets from the RunResDB

        """
        result = []
        if not self.runresLoaded:
            return result

        dbDict = self._RunResDB.toDictionary()
        
        datasets = dbDict[self.taskAttrs['Name']]['Output']['Datasets']
        for dataset in datasets.values():
            outputDict = {}
            for key, value in dataset.items():
                if key in ("PhysicalFileName", "LogicalFileName"):
                    continue
                if value == []:
                    continue
                if len(value) == 1:
                    outputDict[key] = value[0]
                else:
                    outputDict[key] = value
            result.append(outputDict)
        return result
        
        
    def outputCatalogs(self):
        """
        _outputCatalogs_

        Retrieve a list of output catalogs from the RunResDB
        
        """
        result = []
        if not self.runresLoaded:
            return result
        dbDict = self._RunResDB.toDictionary()
        catalogs = dbDict[self.taskAttrs['Name']]['Output']['Catalogs']
        for value in catalogs.values():
            result.append(os.path.basename(value[0]))
        return result

    def inputSource(self):
        """
        _inputSource_

        Get a dictionary of information about the input source from
        the RunResDB

        """
        result = {}
        if not self.runresLoaded:
            return result
        dbDict = self._RunResDB.toDictionary()
        inputParams = dbDict[self.taskAttrs['Name']]['Input']
        for key, value in inputParams.items():
            if len(value) == 0:
                continue
            if len(value) == 1:
                result[key] = value[0]
            else:
                result[key] = value        
        return result
        

    def listFiles(self, catalog):
        """
        _listFiles_

        For the catalog file provided, read the catalog and generate
        a list of FwkJobRep.FileInfo dictionaries

        """
        if not os.path.exists(catalog):
            return []
        return readCatalog(catalog)
    

    def assignFilesToDatasets(self):
        """
        _assignFilesToDatasets_

        Match each file in the job report with the parameters describing the
        dataset that it belongs to.

        This is done by matching catalogs to datasets initially, pending
        the FrameworkJobReport generating the file info directly

        """
        
        datasets = self.outputDatasets()
        for dataset in datasets:
            catalogEntry = dataset.get("Catalog", None)
            if catalogEntry == None:
                continue
            catalog = os.path.join(self.dir, catalogEntry)
            fileList = self.listFiles(catalog)
            for fileEntry in fileList:
                fileEntry['PFN'] = fileEntry['PFN'][0]
                fileInfo = self._JobReport.newFile()
                fileInfo.dataset = dataset
                fileInfo.update(fileEntry)
        return
        
    

    def generateFileStats(self):
        """
        _generateFileStats_

        For each File in the job report, if the file exists, record its
        size and cksum value

        NOTE: Also sets number of events using the inputSource details
        this is a TEMPORARY measure until the details appear in the
        job report

        """
        inpSrc = self.inputSource()
        numEvents = int(inpSrc.get("MaxEvents", 0))
        for fileInfo in self._JobReport.files:
            pfn = fileInfo['PFN']
            if pfn.startswith("file:"):
                pfn = pfn.replace("file:", "")
            if not os.path.exists(pfn):
                continue
            size = os.stat(pfn)[6]
            fileInfo['Size'] = size
            fileInfo['Checksum'] = readCksum(pfn)
            fileInfo['TotalEvents'] = numEvents
        return
    
    def reportFiles(self):
        """
        _reportFiles_

        Return a list of FileInfo objects from the JobReport
        
        """
        result = []
        if not self.jobReportLoaded:
            return result

        return self._JobReport.files
        


def readCksum(filename):
    """
    _readCksum_

    Run a cksum command on a file an return the checksum value

    """
    pop = popen2.Popen4("cksum %s" % filename)
    while pop.poll() == -1:
        exitStatus = pop.poll()
    exitStatus = pop.poll()
    if exitStatus:
        return None
    content = pop.fromchild.read()
    value = content.split()[0]
    return value

            
