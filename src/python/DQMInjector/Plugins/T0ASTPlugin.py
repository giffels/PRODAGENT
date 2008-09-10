#!/usr/bin/env python
"""
_T0ASTPlugin_

Plugin to pull in files for a dataset/run from the Tier 0 DB
and generate a DQM Harvesting workflow/job

"""
import logging
import os

from DQMInjector.Plugins.BasePlugin import BasePlugin
from DQMInjector.HarvestWorkflow import createHarvestingWorkflow

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker


class T0ASTRack:
    """
    _T0ASTRack_

    Wrapper for T0AST API and imports

    """
    def __init__(self, run, primaryDataset):
        self.run = run
        self.primary = primaryDataset



    def listFiles(self):
        """
        _listFiles_

        return a list of LFNs for the run/dataset provided

        """
        return [
            "/store/data/Commissioning08/Cosmics/RECO/CRUZET4_v1/000/058/731/683755BB-5D72-DD11-AF6E-000423D952C0.root",
            "/store/data/Commissioning08/Cosmics/RECO/CRUZET4_v1/000/058/731/205E050F-5D72-DD11-AA90-000423D6C8E6.root",
            "/store/data/Commissioning08/Cosmics/RECO/CRUZET4_v1/000/058/731/AE597F53-6572-DD11-8590-000423D99660.root",
            "/store/data/Commissioning08/Cosmics/RECO/CRUZET4_v1/000/058/731/3A499EC5-5D72-DD11-99DA-001617E30D0A.root",
            "/store/data/Commissioning08/Cosmics/RECO/CRUZET4_v1/000/058/731/D6974AB0-7272-DD11-B4C1-000423D6AF24.root"
            ]



    def cmsswVersion(self):
        """
        _cmsswVersion_

        return the CMSSW Version used for the run/dataset provided

        """
        return "CMSSW_2_1_7"
    

    def globalTag(self):
        """
        _globalTag_

        return the global Tag used for the run/dataset provided

        """
        return "CRUZET4_V2P::All"
    
            



        

class T0ASTPlugin(BasePlugin):

    def __init__(self):
        BasePlugin.__init__(self)
        
        

    def __call__(self, collectPayload):
        """
        _operator(collectPayload)_

        Given the dataset and run in the payload, callout to T0AST
        to find the files to be harvested

        """
        msg = "T0ASTPlugin invoked for %s" % str(collectPayload)
        logging.info(msg)

        #  //
        # // There is only one location for the T0
        #//
        site = "srm.cern.ch" 
        
        baseCache = os.path.join(self.args['ComponentDir'],
                                 "T0ASTPlugin")
        if not os.path.exists(baseCache):
            os.makedirs(baseCache)

        datasetCache = os.path.join(baseCache,
                                    collectPayload['PrimaryDataset'],
                                    collectPayload['ProcessedDataset'],
                                    collectPayload['DataTier'])
        
        if not os.path.exists(datasetCache):
            os.makedirs(datasetCache)

        workflowFile = os.path.join(
            datasetCache,
            "%s-%s-%s-DQMHarvest-Workflow.xml" % (
            collectPayload['PrimaryDataset'],
            collectPayload['ProcessedDataset'],
            collectPayload['DataTier'])
            )
        
        
        t0ast = T0ASTRack(collectPayload['RunNumber'],
                          collectPayload['PrimaryDataset'])
        
        if not os.path.exists(workflowFile):
            msg = "No workflow found for dataset: %s\n " % (
                collectPayload.datasetPath(),)
            msg += "Looking up software version and generating workflow..."

            if self.args.get("OverrideGlobalTag", None) == None:
                globalTag = t0ast.globalTag()
            else:
                globalTag = self.args['OverrideGlobalTag']


            if self.args.get("OverrideCMSSW", None) != None:
                cmsswVersion = self.args['OverrideCMSSW']
            else:
                cmsswVersion = t0ast.cmsswVersion()
            
            workflowSpec = createHarvestingWorkflow(
                collectPayload.datasetPath(),
                site,
                self.args['CmsPath'],
                self.args['ScramArch'],
                cmsswVersion,
                globalTag,
                self.args['ConfigFile'])
            
            workflowSpec.save(workflowFile)
            msg = "Created Harvesting Workflow:\n %s" % workflowFile
            logging.info(msg)
            self.publishWorkflow(workflowFile, workflowSpec.workflowName())
        else:
            msg = "Loading existing workflow for dataset: %s\n " % (
                collectPayload.datasetPath(),)
            msg += " => %s\n" % workflowFile
            logging.info(msg)

            workflowSpec = WorkflowSpec()
            workflowSpec.load(workflowFile)


        job = {}
        jobSpec = workflowSpec.createJobSpec()
        jobName = "%s-%s" % (
            workflowSpec.workflowName(),
            collectPayload['RunNumber']
            )

        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = collectPayload['RunNumber']
        jobSpec.addWhitelistSite(site)
        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.cfgInterface.inputFiles.extend(t0ast.listFiles())
        
        specCacheDir =  os.path.join(
            datasetCache, str(int(collectPayload['RunNumber']) // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)
        
        jobSpec.save(jobSpecFile)


        job["JobSpecId"] = jobName
        job["JobSpecFile"] = jobSpecFile
        job['JobType'] = "Processing"
        job["WorkflowSpecId"] = workflowSpec.workflowName(),
        job["WorkflowPriority"] = 10
        job["Sites"] = [site]
        job["Run"] = collectPayload['RunNumber']
        job['WorkflowSpecFile'] = workflowFile

        msg = "Harvesting Job Created for\n"
        msg += " => Run:       %s\n" % collectPayload['RunNumber']
        msg += " => Primary:   %s\n" % collectPayload['PrimaryDataset']
        msg += " => Processed: %s\n" % collectPayload['ProcessedDataset']
        msg += " => Tier:      %s\n" % collectPayload['DataTier']
        msg += " => Workflow:  %s\n" % job['WorkflowSpecId']
        msg += " => Job:       %s\n" % job['JobSpecId']
        msg += " => Site:      %s\n" % job['Sites']
        logging.info(msg)
        
        return [job]
            
