#!/usr/bin/env python
"""
_TaskState_

Runtime interface for reading in the state of a Task by reading the
RunResDB.xml and FrameworkJobReport.xml files and providing an
API to access the contents of them.

The object is instantiated with a directory that contains the task.


"""

__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: TaskState.py,v 1.8 2006/07/20 21:48:09 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os
import popen2


from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from RunRes.RunResComponent import RunResComponent
from RunRes.RunResDBAccess import loadRunResDB

from FwkJobRep.ReportParser import readJobReport
from FwkJobRep.CatalogParser import readCatalog
from FwkJobRep.SiteLocalConfig import loadSiteLocalConfig


lfnSearch = lambda fileInfo, lfn:  fileInfo.get("LFN", None) == lfn


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
        self._CatalogEntries = None
        self._SiteConfig = None
        self.runresLoaded = False
        self.jobReportLoaded = False
        self.catalogsLoaded = False
        self.siteConfigLoaded = False
        

    def taskName(self):
        """
        _taskName_

        get the task name attribute

        """
        return self.taskAttrs['Name']


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


    def configurationDict(self):
        """
        _configurationDict_

        Return the RunResDB for this task name as a dictionary

        """
        try:
            result = self._RunResDB.toDictionary()[self.taskName()]
        except StandardError, ex:
            result = {}
        return result

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
    

    def dumpJobReport(self):
        """
        _dumpJobReport_

        Read the Job Report file and dump it to stdout

        """
        print "======================Dump Job Report======================"
        if os.path.exists(self.jobReport):
            handle = open(self.jobReport, 'r')
            print handle.read()
        else:
            print "NOT FOUND: %s" % self.jobReport
        print "======================End Dump Job Report======================"
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
    


    def loadCatalogs(self):
        """
        _loadCatalogs_

        Load information from all the output catalogs

        """
        self._CatalogEntries = []
        for catalog in self.outputCatalogs():
            self._CatalogEntries.extend(self.listFiles(catalog))
        self.catalogsLoaded = True
        return
                                        
    def loadSiteConfig(self):
        """
        _loadSiteConfig_

        Load the Site config into this state object

        """
        try:
            self._SiteConfig = loadSiteLocalConfig()
            self.siteConfigLoaded = True
        except StandardError, ex:
            msg = "Unable to load SiteLocalConfig:\n"
            msg += str(ex)
            print msg
            self._SiteConfig = None
        return

    def getSiteConfig(self):
        """
        _getSiteConfig_

        Return the SiteLocalConfig instance if available, None if
        isnt

        """
        if not self.siteConfigLoaded:
            self.loadSiteConfig()
        return self._SiteConfig
    
            

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

        for primaryKey, primaryValue in datasets.items():
            if type(primaryValue) != type({}): continue
            for dataTier, dataTierValue in primaryValue.items():
                if type(dataTierValue) != type({}): continue
                for processedDS, datasetContents in dataTierValue.items():
                    print "Found Dataset: /%s/%s/%s" % (
                        primaryKey, dataTier, processedDS,
                        )
                    for dataKey, dataValue in datasetContents.items():
                        if len(dataValue) == 0:
                            datasetContents[dataKey] = None
                        if len(dataValue) == 1:
                            datasetContents[dataKey] = dataValue[0]
                    result.append(datasetContents)
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
            if key == "InputFiles":
                inputFiles = value['InputFile']
                result['InputFiles'] = inputFiles
                continue
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

        Matching is done by matching OutputModuleName in dataset to
        ModuleLabel for the File entry
        
        """
        
        datasets = self.outputDatasets()
        datasetMap = {}
        for dataset in datasets:
            outModName = dataset.get("OutputModuleName", None)
            if outModName != None:
                if not datasetMap.has_key(outModName):
                    datasetMap[outModName] = []
                datasetMap[outModName].append(dataset)
            
            
        for fileInfo in self._JobReport.files:
            outModLabel = fileInfo.get("ModuleLabel", None)
            if outModLabel == None:
                continue
            if datasetMap.has_key(outModLabel):
                fileInfo.dataset = datasetMap[outModLabel]
                msg = "File: %s\n" % fileInfo['LFN']
                msg += "Produced By Output Module: %s\n" % outModLabel
                msg += "Associated To Datasets:\n"
                for ds in fileInfo.dataset:
                    msg += " ==> /%s/%s/%s\n" % (
                        ds['PrimaryDataset'],
                        ds['DataTier'],
                        ds['ProcessedDataset'],
                        )
                print msg
                
        return
        
    

    def generateFileStats(self):
        """
        _generateFileStats_

        For each File in the job report, if the file exists, record its
        size and cksum value

        """
        if not self.catalogsLoaded:
            self.loadCatalogs()
       
        for fileInfo in self._JobReport.files:
            pfn = fileInfo['PFN']
            lfn = fileInfo['LFN']
            matchedFile = {}
            matchedCatFiles = \
                 [i for i in self._CatalogEntries if lfnSearch(i, lfn)]
            if len(matchedCatFiles) > 0:
                matchedFile = matchedCatFiles[-1]

            if matchedFile.has_key("GUID"):
                fileInfo['GUID'] = matchedFile['GUID']
            
            
            if pfn.startswith("file:"):
                pfn = pfn.replace("file:", "")
            if not os.path.exists(pfn):
                continue
            size = os.stat(pfn)[6]
            fileInfo['Size'] = size
            fileInfo.addChecksum("cksum", readCksum(pfn))
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
    value = content.strip()
    value = content.split()[0]
    return value

            
