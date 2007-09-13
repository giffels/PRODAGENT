#!/usr/bin/env python
"""
_RunResTools_

Tools for adding standard RunRes Runtime lookup objects to TaskObjects


"""
import os

from RunRes.RunResComponent import RunResComponent
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc

from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetDetails
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasets
from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.MCPayloads.MergeTools import getSizeBasedMergeDatasetsFromNode


def unquote(strg):
    """remove leading and trailing quotes from string"""
    while strg.startswith("\'") or strg.startswith("\""):
        strg = strg[1:]
    while strg.endswith("\'") or strg.endswith("\""):
        strg = strg[:-1]
    return strg

    


class InstallRunResComponent:
    """
    _InstallRunResComponent_

    Install a RunResComponent instance in a TaskObject.
    Create a basic structure for the RunResComponent, but do not
    populate it with data

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Install the RunResDB key with a RunResComponent instance as the
        value, add it to the list of XML IMProvNodes to be written out

        Create basic structure in RunResComponent common to all tasks
        
        """
        newComponent = RunResComponent()
        objName = taskObject['Name']
        workflow = taskObject['RequestName']
        jobspec = taskObject['JobName']
        jobtype = taskObject['JobType']
        

        newComponent.addPath(objName)
        newComponent.addData("/%s/WorkflowSpecID" % objName, workflow)
        newComponent.addData("/%s/JobSpecID" % objName, jobspec)
        newComponent.addData("/%s/JobType" % objName, jobtype)
        newComponent.addPath("/%s/Type" % objName)
        newComponent.addPath("/%s/Configuration" % objName)
        newComponent.addPath("/%s/Application" % objName)
        newComponent.addPath("/%s/Input" % objName)
        newComponent.addPath("/%s/Output" % objName)
        newComponent.addPath("/%s/SizeBasedMerge" % objName)


        taskObject['RunResDB'] = newComponent
        taskObject['IMProvDocs'].append("RunResDB")
        return


class CMSSWRunResDB:
    """
    _CMSSWRunResDB_

    Insert information about a CMSSW Type JobSpecNode into its TaskObjects
    RunResComponent instance.

    This makes it easy to find output, catalogs, job reports etc at runtime.

    """
    def __init__(self, **compArgs):
        self.args = compArgs
        self.mergeThresh = self.args.get("MinMergeFileSize", 2000000000)
        self.doSizeMerge = self.args.get("SizeBasedMerge", False)
        if str(self.doSizeMerge).lower() == "true":
            self.doSizeMerge = True
        else:
            self.doSizeMerge = False
    
    def __call__(self, taskObject):
        """
        _operator()_

        Define operation on a CMSSW type TaskObject

        """
        toType = taskObject.get("Type", None)
        if toType != "CMSSW":
            return
        runresComp = taskObject['RunResDB']
        objName = taskObject['Name']

        #  // 
        # // Application Data
        #// 
        runresComp.addData("/%s/Application/Executable" % objName, 
                           taskObject['CMSExecutable'])
        runresComp.addData("/%s/Configuration/CfgFile" % objName,
                           "PSet.py")
        runresComp.addData("/%s/Configuration/PyCfgFile" % objName,
                           "PSet.py")

        runresComp.addData("/%s/Output/FrameworkJobReport" % objName,
                           os.path.join("$PRODAGENT_JOB_DIR",
                                        taskObject['RuntimeDirectory'],
                                        "FrameworkJobReport.xml"))
        newComponent.addData("/%s/SizeBasedMerge/DoSizeMerge" % objName,
                             self.doSizeMerge)
        newComponent.addData("/%s/SizeBasedMerge/MinMergeFileSize" % objName,
                             self.mergeThresh)
        #  //
        # // Datasets
        #//
        payloadNode = taskObject.get("JobSpecNode", None)
        if payloadNode == None:
            payloadNode = taskObject["PayloadNode"]

        runresComp.addPath("/%s/Output/Datasets" % objName)
        datasets = getOutputDatasetDetails(payloadNode)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(payloadNode))
        for dataset in datasets:
            if dataset['DataTier'] == "":
                continue
            dsPath = "/%s/Output/Datasets%s" % (
                objName, dataset.name())
            runresComp.addPath(dsPath)
            for key, val in dataset.items():
                runresComp.addData("/%s/%s" % (dsPath, key), unquote(str(val)))
        #  //
        # // Output Catalogs
        #//
        runresComp.addPath("/%s/Output/Catalogs" % objName)
        cfgInt = payloadNode.cfgInterface
        for modName, item in cfgInt.outputModules.items():
            if item.get('catalog', None) == None:
                continue
            catalog = unquote(item['catalog'])
            catPath = "/%s/Output/Catalogs/%s" % (objName, modName)
            runresComp.addData(
                catPath,
                os.path.join("$PRODAGENT_JOB_DIR",
                             taskObject['RuntimeDirectory'],
                             catalog)
                )
            
        #  //
        # // Number of Events from Source
        #//
        cfgInt = payloadNode.cfgInterface
        inpSrc = cfgInt.sourceParams
        runresComp.addData("/%s/Input/SourceType" % objName, cfgInt.sourceType)
        
        runresComp.addData("/%s/Input/MaxEvents" % objName,
                           cfgInt.maxEvents['input'])
        if inpSrc.has_key('firstRun'):
            runresComp.addData("/%s/Input/FirstRun" % objName,
                               inpSrc['firstRun'])
        runresComp.addPath("/%s/Input/InputFiles" % objName)
        #  //
        # // List of input files
        #//
        inpFileList = cfgInt.inputFiles

        for inpFile in inpFileList:
            runresComp.addData("/%s/Input/InputFiles/InputFile" % objName,
                               inpFile.replace("\'", ""))
        return

class InsertDirInRunRes:
    """
    _InsertDirInRunRes_

    Insert the Runtime directory into the RunResDB

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Insert the runtime directory name/path into the RunResDB

        """
        if not taskObject.has_key("RunResDB"):
            return

        if taskObject.has_key("RuntimeDirectory"):
            runresComp = taskObject['RunResDB']
            runresComp.addData(
                "/%s/Directory" % taskObject['Name'],
                taskObject['RuntimeDirectory']
                )
        return
    
class BulkCMSSWRunResDB:
    """
    _CMSSWRunResDB_

    Insert information about a CMSSW Type JobSpecNode into its TaskObjects
    RunResComponent instance.

    This makes it easy to find output, catalogs, job reports etc at runtime.

    """
    def __init__(self, **compArgs):
        self.args = compArgs
        self.mergeThresh = self.args.get("MinMergeFileSize", 2000000000)
        self.doSizeMerge = self.args.get("SizeBasedMerge", False)
        if str(self.doSizeMerge).lower() == "true":
            self.doSizeMerge = True
        else:
            self.doSizeMerge = False
            
    def __call__(self, taskObject):
        """
        _operator()_

        Define operation on a CMSSW type TaskObject

        """
        toType = taskObject.get("Type", None)
        if toType != "CMSSW":
            return
        runresComp = taskObject['RunResDB']
        objName = taskObject['Name']

        #  // 
        # // Application Data
        #// 
        runresComp.addData("/%s/Application/Executable" % objName, 
                           taskObject['CMSExecutable'])
        runresComp.addData("/%s/Configuration/CfgFile" % objName,
                           "PSet.py")
        runresComp.addData("/%s/Configuration/PyCfgFile" % objName,
                           "PSet.py")

        runresComp.addData("/%s/Output/FrameworkJobReport" % objName,
                           os.path.join("$PRODAGENT_JOB_DIR",
                                        taskObject['RuntimeDirectory'],
                                        "FrameworkJobReport.xml"))
        runresComp.addData("/%s/SizeBasedMerge/DoSizeMerge" % objName,
                           self.doSizeMerge)
        runresComp.addData("/%s/SizeBasedMerge/MinMergeFileSize" % objName,
                           self.mergeThresh)
        #  //
        # // Datasets
        #//
        payloadNode = taskObject["PayloadNode"]
            
        runresComp.addPath("/%s/Output/Datasets" % objName)
        datasets = getOutputDatasets(payloadNode)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(payloadNode))
        for dataset in datasets:
            if dataset['DataTier'] == "":
                continue
            dsPath = "/%s/Output/Datasets%s" % (
                objName, dataset.name())
            runresComp.addPath(dsPath)
            for key, val in dataset.items():
                runresComp.addData("/%s/%s" % (dsPath, key), unquote(str(val)))
        #  //
        # // Output Catalogs
        #//
        runresComp.addPath("/%s/Output/Catalogs" % objName)
        
        
        return
            
        
class AccumulateRunResDB:
    """
    _AccumulateRunResDB_

    Traverse all the RunResDB entries in a TaskObject tree and
    store the list of locations to generate a master RunResDB file
    in the toplevel of the job.

    """
    def __init__(self):
        self.runresFiles = []
        
    def __call__(self, taskObject):
        """
        _operator()_

        Extract the location of the RunResDB xml file from the
        TaskObject being processed

        """
        if not taskObject.has_key("RunResDB"):
            return
      
        
        runresFile = "file://$PRODAGENT_JOB_DIR/"
        runresFile += "%s/RunResDB.xml" % taskObject['RuntimeDirectory']
        self.runresFiles.append(runresFile)
        return

    
        

    def writeMainDB(self, targetFile):
        """
        _writeMainDB_

        Write the main RunResDB file in the location provided
        so that all the components can be welded into a single RunResDB
        structure

        """
        db = IMProvDoc("RunResDB")
        for url in self.runresFiles:
            entry = IMProvNode("RunResDBURL", None, URL = url)
            db.addNode(entry)

        handle = open(targetFile, 'w')
        handle.write(db.makeDOMDocument().toprettyxml())
        handle.close()
        return
    
