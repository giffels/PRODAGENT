#!/usr/bin/env python
"""
_RuntimePSetPrep_

Runtime script that reads in the python PSet file, and
writes out the {{{}}} format PSet file.

May also require some localisation of parameters, expansion
of env vars to be done here in support of chained jobs.

"""

import sys
import os
import pickle

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState

from ProdCommon.TrivialFileCatalog import TrivialFileCatalog

class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance

def unquote(strg):
    """remove leading and trailing quotes from string"""
    while strg.startswith("\'") or strg.startswith("\""):
        strg = strg[1:]
    while strg.endswith("\'") or strg.endswith("\""):
        strg = strg[:-1]
    return strg   


class JobSpecExpander:

    def __init__(self, jobSpecFile):
        self.jobSpec = JobSpec()
        self.jobSpec.load(jobSpecFile)
        self.taskState = TaskState(os.getcwd())
        self.taskState.loadRunResDB()
        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(os.environ["PRODAGENT_WORKFLOW_SPEC"])
        
        self.config = self.taskState.configurationDict()

        finder = NodeFinder(self.taskState.taskName())
        self.jobSpec.payload.operate(finder)
        self.jobSpecNode = finder.result

        wffinder = NodeFinder(self.taskState.taskName())
        self.workflowSpec.payload.operate(wffinder)
        self.workflowNode = wffinder.result

        if self.jobSpecNode.jobType != "Merge":
            if self.config.has_key('Configuration'):
                try:
                    self.createPSet()
                except Exception, ex:
                    msg = "Unable to generate cmsRun Config from JobSpec:\n"
                    msg += str(ex)
                    print msg
                    badfile = open("exit.status", 'w')
                    badfile.write("10040")
                    badfile.close()
        else:
            #  //
            # // Merge job
            #//
            self.createMergePSet()
        
            
    def handleInputLink(self, config, inpLink):
        """
        _handleInputLink_
                                                                                                                         
        Generate the information for the input link between this
        task and the task specified
                                                                                                                         
        """
        msg = "Input Link Detected:\n"
        for k, v in inpLink.items():
            msg += " %s = %s\n" % (k,v)
        print msg
                                                                                                                         
        inputTask = getTaskState(inpLink['InputNode'])
                                                                                                                         
        if inputTask == None:
            msg = "Unable to create InputLink for task: %s\n" % (
                inpLink['InputNode'],)
            msg += "Input TaskState could not be retrieved..."
            raise RuntimeError, msg
                                                                                                                         
        inputTask.loadJobReport()
        inputReport = inputTask.getJobReport()
        if inputReport == None:
            msg = "Unable to create InputLink for task: %s\n" % (
                inpLink['InputNode'],)
            msg += "Unable to load input job report file"
            raise RuntimeError, msg
        
        # add files to override catalog
        inputFileList = []
        tfc = None

        for file in inputReport.files:
            if not file['ModuleLabel'] == inpLink['OutputModule']:
                continue
            if file.get('LFN', None) not in (None, '', 'None'):
                if not tfc:
                    tfc = TrivialFileCatalog.TrivialFileCatalog()
                inputFileList.append(file['LFN'])
                tfc.addLfnToPfnRule('override', file['LFN'], file['PFN'])
            else:
                inputFileList.append("file:%s" % file['PFN'])

        if tfc:
            print "Creating override tfc, contents below"
            print str(tfc)
            tfc.write(os.path.join(os.getcwd(), 'override_catalog.xml'))        
                                                                                                                         
        if inpLink['InputSource'] == "source":
            #  //
            # // feed into main source
            #//
            config.inputFiles = inputFileList
            if tfc:
                config.inputOverrideCatalog = os.path.join(os.getcwd(), 'override_catalog.xml')
                
            msg = "Input Link created to input source for files:\n"
            for f in inputFileList:
                msg += " %s\n" % f
                                                                                                                         
            print msg
            return
        #  //
        # // Need to add to secondary source with name provided
        #//
        raise NotImplementedError, "Havent implemented secondary source input links at present..."


    def createPSet(self):
        """
        _createPSet_

        Create the PSet cfg File

        """
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.py")[0]
        cfgFile = str(cfgFile)

    
        self.jobSpecNode.loadConfiguration()
        self.jobSpecNode.cfgInterface.rawCfg = self.workflowNode.cfgInterface.rawCfg

        for inpLink in self.jobSpecNode._InputLinks:
            #  //
            # // We have in-job input links to be resolved
            #//
            self.handleInputLink(self.jobSpecNode.cfgInterface, inpLink)

        cmsProcess = self.jobSpecNode.cfgInterface.makeConfiguration()

        cfgDump = open("CfgFileDump.log", 'w')
        cfgDump.write(cmsProcess.dumpConfig())
        cfgDump.close()
        
        
        handle = open(cfgFile, 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(cmsProcess))
        handle.write("process = pickle.loads(pickledCfg)\n")
        handle.close()
        
        return
        

    def createMergePSet(self):
        """
        _createMergePSet_

        Merges are a little different since we have to build the entire
        process object from scratch.

        """
        print "<<<<<<<<<<<<<<<<<<<<Merge>>>>>>>>>>>>>>>>>>>>>."
        cfgFile = self.config['Configuration'].get("CfgFile", "PSet.py")[0]
        cfgFile = str(cfgFile)
        self.jobSpecNode.loadConfiguration()
        cfgInt = self.jobSpecNode.cfgInterface

        from FWCore.ParameterSet.Config import Process, EndPath
        from FWCore.ParameterSet.Modules import OutputModule, Source
        import FWCore.ParameterSet.Types as CfgTypes

        process = Process("Merge")
        process.source = Source("PoolSource")
        process.source.fileNames = CfgTypes.untracked(CfgTypes.vstring())
        for entry in cfgInt.inputFiles:
            process.source.fileNames.append(str(entry))
                

        outMod = cfgInt.outputModules['Merged']
        process.Merged = OutputModule("PoolOutputModule")
        process.Merged.fileName = CfgTypes.untracked(CfgTypes.string(
            outMod['fileName']))

        process.Merged.logicalFileName = CfgTypes.untracked(CfgTypes.string(
            outMod['logicalFileName']))

        process.Merged.catalog = CfgTypes.untracked(CfgTypes.string(
            outMod['catalog']))
        process.outputPath = EndPath(process.Merged)
        cfgDump = open("CfgFileDump.log", 'w')
        cfgDump.write(process.dumpConfig())
        cfgDump.close()
        
        
        handle = open(cfgFile, 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(process))
        handle.write("process = pickle.loads(pickledCfg)\n")
        handle.close()
        return


if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    
    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    workflowSpec = os.environ.get("PRODAGENT_WORKFLOW_SPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if workflowSpec == None:
        msg = "Unable to find WorkflowSpec from PRODAGENT_WORKFLOW_SPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg
    

    if not os.path.exists(jobSpec):
        msg += "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = JobSpecExpander(jobSpec)
    
    
    












