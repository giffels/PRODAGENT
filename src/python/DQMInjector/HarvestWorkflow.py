#!/usr/bin/env python
"""
_HarvestWorkflow_

Generate a Harvesting workflow spec


"""
import time
import os
import logging
import imp

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions


def configOnFly(cmsPath, scramArch, cmsswVersion):
    """
    _configOnFly_

    Generate dummy config for on-the-fly configuration on WN

    """
    loader = CMSSWAPILoader(scramArch,
                            cmsswVersion,
                            cmsPath)

    try:
        loader.load()
    except Exception, ex:
        msg = "Couldn't load CMSSW libraries: %s" % ex
        logging.error(msg)
        raise RuntimeError, msg

    import FWCore.ParameterSet.Config as cms

    process = cms.Process("EDMtoMEConvert")
    process.source = cms.Source("PoolSource",
            fileNames = cms.untracked.vstring()
                                )
    process.configurationMetadata = cms.untracked(cms.PSet())
    process.configurationMetadata.name = cms.untracked(
            cms.string("TEMP_CONFIG_USED"))
    process.configurationMetadata.version = cms.untracked(
        cms.string(cmsswVersion))
    process.configurationMetadata.annotation = cms.untracked(
        cms.string("DQM Harvesting Configuration Placeholder"))
    
    
    cfgWrapper = CMSSWConfig()
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
        msg = "Error loading config file:\n"
        msg += "%s\n" % filename
        msg += "%s" % ex
        
        logging.error(msg)
        loader.unload()
        raise RuntimeError, msg

    cmsCfg = modRef.process
    cfgWrapper = CMSSWConfig()
    cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
    cfgInt.validateForProduction()
    loader.unload()

    return cfgWrapper


def createHarvestingWorkflow(dataset, site, cmsPath, scramArch,
                             cmsswVersion, globalTag, configFile = None,
                             DQMServer = None, proxyLocation = None, 
                             DQMCopyToCERN = None, runNumber = None):
    """
    _createHarvestingWorkflow_

    Create a Harvesting workflow to extract DQM information from
    a dataset

    Enters an essentially empty process that will be updated
    at runtime to use the harvesting cfg from the release.

    """

    datasetPieces = DatasetConventions.parseDatasetPath(dataset)

    physicsGroup = "OfflineDQM"
    category = "DQM"
    
    if runNumber == None:
        requestId = "OfflineDQM"
        label = "%s-%s-%s" % (datasetPieces['Primary'], datasetPieces['Processed'],
                          datasetPieces['DataTier'])
        channel = "DQMHarvest"
    else:
        requestId = "%s-%s" % (datasetPieces["Primary"], datasetPieces["DataTier"])
        label = "DQMHarvesting"
        channel = "Run%s" % runNumber

    logging.debug("path, arch, ver: %s, %s, %s" % (cmsPath, scramArch, cmsswVersion))

    if configFile != None:
        cfgWrapper = configFromFile(cmsPath, scramArch,
                                    cmsswVersion, configFile)
    else:
        cfgWrapper = configOnFly(cmsPath, scramArch,
                                 cmsswVersion)
        
    #  //
    # // Pass in global tag
    #//
    cfgWrapper.conditionsTag = globalTag


    maker = WorkflowMaker(requestId, channel, label )
    maker.setCMSSWVersion(cmsswVersion)
    maker.setPhysicsGroup(physicsGroup)
    maker.setConfiguration(cfgWrapper, Type = "instance")
    maker.changeCategory(category)
    maker.setPSetHash("NO_HASH")
    maker.addInputDataset(dataset)
    maker.setActivity('harvesting')

    spec = maker.makeWorkflow()
    spec.parameters['WorkflowType'] = "Harvesting"
    spec.parameters['DBSURL'] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    spec.parameters['OnlySites'] = site
    if DQMServer != None :
        spec.parameters['DQMServer'] = DQMServer
    if proxyLocation != None :
        spec.parameters['proxyLocation'] = proxyLocation
    if DQMCopyToCERN != None :
        spec.parameters['DQMCopyToCERN'] = DQMCopyToCERN

    spec.payload.scriptControls['PostTask'].append(
        "JobCreator.RuntimeTools.RuntimeOfflineDQM")

    if configFile == None:
        preExecScript = spec.payload.scriptControls["PreExe"]
        preExecScript.append("JobCreator.RuntimeTools.RuntimeOfflineDQMSetup")


    return spec


