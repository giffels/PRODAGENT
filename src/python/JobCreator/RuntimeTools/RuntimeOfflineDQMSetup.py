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


class TEMP:
    """
    _TEMP_

    Placeholder for getting the runtime DQM configuration in case
    it isnt in the release

    """

    def makeDQMHarvestingConfigImpl(datasetName, runNumber,  globalTag, *inputFiles, **options):
        """
        _makeDQMHarvestingConfig_
        
        Arguments:
        
        datasetName - aka workflow name for DQMServer, this is the name of the
        dataset containing the harvested run
        runNumber - The run being harvested
        globalTag - The global tag being used
        inputFiles - The list of LFNs being harvested
        
        """
        
        import FWCore.ParameterSet.Config as cms
        
        process = cms.Process("EDMtoMEConvert")
        
        process.load("DQMServices.Components.EDMtoMEConverter_cff")
        
        process.load(
            "Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
        process.GlobalTag.connect = \
             "frontier://PromptProd/CMS_COND_21X_GLOBALTAG"
        process.GlobalTag.globaltag = globalTag
        process.prefer("GlobalTag")
        
        process.load("Configuration.StandardSequences.Geometry_cff")
        
        process.load(
            "DQMOffline.Configuration.DQMOfflineCosmics_SecondStep_cff")
        
        process.maxEvents = cms.untracked.PSet(
            input = cms.untracked.int32(1)
        )
        
        process.options = cms.untracked.PSet(
         fileMode = cms.untracked.string('FULLMERGE')
        )
        
        process.source = cms.Source("PoolSource",
        #    dropMetaData = cms.untracked.bool(True),
            processingMode = cms.untracked.string("RunsLumisAndEvents"),
            fileNames = cms.untracked.vstring()
        )

        for fileName in inputFiles:
            process.source.fileNames.append(fileName)
        
        process.maxEvents.input = -1
        
        process.source.processingMode = "RunsAndLumis"
        process.configurationMetadata = cms.untracked(cms.PSet())
        process.configurationMetadata.name = cms.untracked(
            cms.string("TEMP_CONFIG_USED"))
        process.configurationMetadata.version = cms.untracked(
            cms.string(os.environ['CMSSW_VERSION']))
        process.configurationMetadata.annotation = cms.untracked(
            cms.string("DQM Harvesting Configuration"))
        
        process.DQMStore.referenceFileName = ''
        process.dqmSaver.convention = 'Offline'
        process.dqmSaver.workflow = datasetName
        
        process.DQMStore.collateHistograms = False
        process.EDMtoMEConverter.convertOnEndLumi = True
        process.EDMtoMEConverter.convertOnEndRun = False
        
        process.p1 = cms.Path(
            process.EDMtoMEConverter*process.DQMOfflineCosmics_SecondStep*process.dqmSaver)

        return process
    makeDQMHarvestingConfig = staticmethod(makeDQMHarvestingConfigImpl)

class TEMP31:
    """
    _TEMP_

    Placeholder for getting the runtime DQM configuration in case
    it isnt in the release

    """

    def makeDQMHarvestingConfigImpl(datasetName, runNumber,  globalTag, *inputFiles, **options):
        """
        _makeDQMHarvestingConfig_
        
        Arguments:
        
        datasetName - aka workflow name for DQMServer, this is the name of the
        dataset containing the harvested run
        runNumber - The run being harvested
        globalTag - The global tag being used
        inputFiles - The list of LFNs being harvested
        
        """
        
        import FWCore.ParameterSet.Config as cms
        
        process = cms.Process('HARVESTING')

	# import of standard configurations
	process.load('Configuration/StandardSequences/Services_cff')
	process.load('FWCore/MessageService/MessageLogger_cfi')     
	process.load('Configuration/StandardSequences/MixingNoPileUp_cff')
	process.load('Configuration/StandardSequences/GeometryIdeal_cff') 
	process.load('Configuration/StandardSequences/MagneticField_38T_cff')
	process.load('Configuration/StandardSequences/EDMtoMEAtRunEnd_cff')  
	process.load('Configuration/StandardSequences/HarvestingCosmics_cff')
	process.load('Configuration/StandardSequences/FrontierConditions_GlobalTag_cff')

	process.GlobalTag.connect = \
             "frontier://FrontierProd/CMS_COND_31X_GLOBALTAG"

	#process.configurationMetadata = cms.untracked.PSet(
	    #version = cms.untracked.string('$Revision: 1.3 $'),
	    #annotation = cms.untracked.string('step3_DT2_1 nevts:1'),
	    #name = cms.untracked.string('PyReleaseValidation')
	#)
	process.maxEvents = cms.untracked.PSet(
	    input = cms.untracked.int32(1)
	)
	process.options = cms.untracked.PSet(
	    Rethrow = cms.untracked.vstring('ProductNotFound'),
	    fileMode = cms.untracked.string('FULLMERGE')
	)
        process.source = cms.Source("PoolSource",
        #    dropMetaData = cms.untracked.bool(True),
            processingMode = cms.untracked.string("RunsLumisAndEvents"),
            fileNames = cms.untracked.vstring()
        )

        for fileName in inputFiles:
            process.source.fileNames.append(fileName)
        
        process.maxEvents.input = -1
        
        process.source.processingMode = "RunsAndLumis"
        process.configurationMetadata = cms.untracked(cms.PSet())
        process.configurationMetadata.name = cms.untracked(
            cms.string("TEMP_CONFIG_USED"))
        process.configurationMetadata.version = cms.untracked(
            cms.string(os.environ['CMSSW_VERSION']))
        process.configurationMetadata.annotation = cms.untracked(
            cms.string("DQM Harvesting Configuration"))
	# Additional output definition

	# Other statements
	process.GlobalTag.globaltag = globalTag

        #process.DQMStore.referenceFileName = ''
        #process.dqmSaver.convention = 'Offline'
        process.dqmSaver.workflow = datasetName
        
        #process.DQMStore.collateHistograms = False
        #process.EDMtoMEConverter.convertOnEndLumi = True
        #process.EDMtoMEConverter.convertOnEndRun = False

	# Path and EndPath definitions
	process.edmtome_step = cms.Path(process.EDMtoME)
	process.validationHarvesting = cms.Path(process.postValidation*process.hltpostvalidation)
	process.dqmHarvesting = cms.Path(process.DQMOfflineCosmics_SecondStep*process.DQMOfflineCosmics_Certification)
	process.dqmsave_step = cms.Path(process.DQMSaver)

	# Schedule definition
	process.schedule = cms.Schedule(process.edmtome_step,process.dqmHarvesting,process.dqmsave_step)
        return process
    makeDQMHarvestingConfig = staticmethod(makeDQMHarvestingConfigImpl)



        


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

        configCreator = self.importConfigurationLibrary()
        
        try:
            process = configCreator(
                self.inputDataset.name(),
                self.runNumber,
                self.globalTag,
                *self.inputFiles)
        except Exception, ex:
            msg = "Error creating harvesting configuration\n"
            msg += str(ex)
            print msg
            raise RuntimeError, "Harvesting Config Failure"

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
        harvestingModule = "Configuration.GlobalRuns.HarvestingConfig"
        try:
            msg = "Attempting Import of: %s" % harvestingModule
            print msg
            from Configuration.GlobalRuns.HarvestingConfig import makeDQMHarvestingConfig
        except Exception, ex:
            msg = "Unable to import %s\n" % harvestingModule
            msg += "%s\n" % str(ex)
            msg += "Falling Back to local definition"
            print msg
	    cmsswVersions = os.environ['CMSSW_VERSION'].split('_')
	    if (cmsswVersions[1]=='3'):
              makeDQMHarvestingConfig = TEMP31.makeDQMHarvestingConfig
            else:
	      makeDQMHarvestingConfig = TEMP.makeDQMHarvestingConfig

        return makeDQMHarvestingConfig
    


        

    
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

