#!/usr/bin/env python
"""
_HarvestWorkflow_

Generate a Harvesting workflow spec


"""
import time
import os
import logging


import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions


def configFromScratch(cmsPath, scramArch, cmsswVersion):
    """
    _configFromScratch_

    Empty process that will get updated from the release

    """
    loader = CMSSWAPILoader(scramArch,
                            cmsswVersion,
                            cmsPath)
    cfgWrapper = CMSSWConfig()
    try:
        loader.load()
    except Exception, ex:
        msg = "Couldn't load CMSSW libraries: %s" % ex
        logging.error(msg)
        raise RuntimeError, msg

    import FWCore.ParameterSet.Config as cms
    import FWCore.ParameterSet.Types as CmsTypes
    process = cms.Process("EDMtoMEConvert")
    process.maxEvents = cms.untracked.PSet(
        input = cms.untracked.int32(1)
        )

    process.options = cms.untracked.PSet(
        fileMode = cms.untracked.string('FULLMERGE')
        )

    process.source = cms.Source(
        "PoolSource",

        processingMode = cms.untracked.string("RunsLumisAndEvents"),
        fileNames = cms.untracked.vstring('file:reco2.root')
        )

    process.maxEvents.input = -1

    process.source.processingMode = "RunsAndLumis"



    configName = "dqm-harvesting"
    configVersion = "harvesting-%s" % cmsswVersion
    configAnnot = "auto generated dqm harvesting config"

    process.configurationMetadata = CmsTypes.untracked(CmsTypes.PSet())
    process.configurationMetadata.name = CmsTypes.untracked(
        CmsTypes.string(configName))
    process.configurationMetadata.version = CmsTypes.untracked(
        CmsTypes.string(configVersion))
    process.configurationMetadata.annotation = CmsTypes.untracked(
        CmsTypes.string(configAnnot))

    cfgInt = cfgWrapper.loadConfiguration(process)

    cfgInt.validateForProduction()



    loader.unload()
    return cfgWrapper

def configFromFile(cmsPath, scramArch, cmsswVersion, filename):
    """
    _configFromFile_

    Override normal config from scratch/runtime with a specific
    file.

    Useful for tests/debugging etc

    """
    cfgBaseName = os.path.basename(filename).replace(".py", "")
    cfgDirName = os.path.dirname(filename)
    modPath = imp.find_module(cfgBaseName, [cfgDirName])

    loader = CMSSWAPILoader(scramArch,
                            cmsswVersion,
                            cmsPath)

    try:
        loader.load()
    except Exception, ex:
        msg = "Couldn't load CMSSW libraries: %s" % ex
        logging.error(msg)
        raise RuntimeError, msg

    try:
        modRef = imp.load_module(cfgBaseName, modPath[0],
                                 modPath[1], modPath[2])
    except Exception, ex:
        msg = "Error loading config file: %s" % ex
        msg += "%s\n" % filename
        logging.error(msg)
        loader.unload()
        raise RuntimeError, msg

    cmsCfg = modRef.process
    cfgWrapper = CMSSWConfig()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()
    loader.unload()

    return cfgWrapper


def createHarvestingWorkflow(dataset, site, cmsPath, scramArch, cmsswVersion,
                             configFile = None):
    """
    _createHarvestingWorkflow_

    Create a Harvesting workflow to extract DQM information from
    a dataset

    Enters an essentially empty process that will be updated
    at runtime to use the harvesting cfg from the release.

    """

    datasetPieces = DatasetConventions.parseDatasetPath(dataset)

    requestId = "OfflineDQM"
    physicsGroup = "OfflineDQM"
    label = "%s-%s-%s" % (datasetPieces['Primary'], datasetPieces['Processed'],
                          datasetPieces['DataTier'])
    category = "DQM"
    channel = "DQMHarvest"

    cfgWrapper = CMSSWConfig()
##    if configFile != None:
##        cfgWrapper = configFromFile(cmsPath, scramArch,
##                                    cmsswVersion, configFile)
##    else:
##        cfgWrapper = configFromScratch(cmsPath, scramArch, cmsswVersion)





    maker = WorkflowMaker(requestId, channel, label )
    maker.setCMSSWVersion(cmsswVersion)
    maker.setPhysicsGroup(physicsGroup)
    maker.setConfiguration(cfgWrapper, Type = "instance")
    maker.changeCategory(category)
    maker.setPSetHash("NO_HASH")
    maker.addInputDataset(dataset)

    spec = maker.makeWorkflow()
    spec.parameters['DBSURL'] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    spec.parameters['OnlySites'] = site
    spec.payload.scriptControls['PostTask'].append(
        "JobCreator.RuntimeTools.RuntimeOfflineDQM")

    if configFile == None:
        preExecScript = spec.payload.scriptControls["PreExe"]
        preExecScript.append("JobCreator.RuntimeTools.RuntimeOfflineDQMSetup")


    return spec


