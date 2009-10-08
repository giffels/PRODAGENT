#!/usr/bin/env python
"""
_RuntimeOfflineDQMSetup_

Runtime script to set up the Offline DQM Harvesting Configuration

"""

import sys
import os
import pickle
from ProdCommon.FwkJobRep.TaskState import TaskState, getTaskState
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from RunRes.RunResComponent import RunResComponent


class NodeFinder:
    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance


class OfflineDQMSetup:
    """
    _OfflineDQMSetup_

    Generate the PSet for the job on the fly

    """
    def __init__(self):
        self.jobSpec = JobSpec()
        self.jobSpec.load(os.environ['PRODAGENT_JOBSPEC'])
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

        self.inputFiles = self.jobSpecNode.cfgInterface.inputFiles
        self.globalTag = self.jobSpecNode.cfgInterface.conditionsTag
        self.inputDataset = self.jobSpecNode._InputDatasets[0]
        self.runNumber = self.jobSpec.parameters['RunNumber']
        self.scenario = self.jobSpec.parameters.get('Scenario', 'relvalmc')
        

    def __call__(self):
        """
        _operator()_

        Invoke the setup tool

        """
        msg = "Creating Harvesting Configuration for:\n"
        msg += " => Dataset: %s\n" % self.inputDataset.name()
        msg += " => Run Number: %s\n" % self.runNumber
        msg += " => Global Tag: %s\n" % self.globalTag
        msg += " => Input Files:\n" 
        for inputfile in self.inputFiles:
            msg += "    => %s\n" % inputfile
        print msg

        process = self.importConfigurationLibrary()
        
        pycfgDump = open("PyCfgFileDump.log", 'w')
        try:
            pycfgDump.write(process.dumpPython())
        except Exception, ex:
            msg = "Error writing python format cfg dump:\n"
            msg += "%s\n" % str(ex)
            msg += "This needs to be reported to the framework team"
            pycfgDump.write(msg)
        pycfgDump.close()

        #  //
        # // Save the edited config as PSet.py
        #//
        handle = open("PSet.py", 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(process))
        handle.write("process = pickle.loads(pickledCfg)\n")
        handle.close()
        print "Wrote PSet.py for harvesting"
        return


    def importConfigurationLibrary(self):
        """
        _importConfigurationLibrary_

        Import the method to create the PSet

        """
        harvestingModule = "Configuration.DataProcessing.GetScenario"
        try:
            msg = "Attempting Import of: %s" % harvestingModule
            print msg
            from Configuration.DataProcessing.GetScenario import getScenario
        except Exception, ex:
            msg = "Unable to import %s\n" % harvestingModule
            msg += "%s\n" % str(ex)
            raise RuntimeError, msg

        try:
            scenario = getScenario(self.scenario)
        except Exception, ex:
            msg = "Error getting Scenario implementation for %s\n" % (
                self.scenario,)
            msg += str(ex)
            raise RuntimeError, msg

        print "Retrieved Scenario: %s" % self.scenario
        print "Using Global Tag: %s" % self.globalTag
        print "Dataset: %s" % self.inputDataset.name()
        print "Run: %s" % self.runNumber

        try:
            process = scenario.dqmHarvesting(self.inputDataset.name(),
                                             self.runNumber,
                                             self.globalTag)
            # We might need this line since the ConfigBuilder is not filling up
            # the global tag
            # process.GlobalTag.globaltag = self.globalTag
        except Exception, ex:
            msg = "Error creating Harvesting config:\n"
            msg += str(ex)
            raise RuntimeError, msg

        process.source.fileNames.extend(self.inputFiles)

        return process


    
if __name__ == '__main__':
    print "=========DQM Harvesting Job Setup================="


    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg
    workflowSpec = os.environ.get("PRODAGENT_WORKFLOW_SPEC", None)
    if workflowSpec == None:
        msg = "Unable to find WorkflowSpec from "
        msg += "PRODAGENT_WORKFLOW_SPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg = "Cannot find JobSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = OfflineDQMSetup()
    instance()
    print "=========DQM Harvest Job Setup Done==========="

