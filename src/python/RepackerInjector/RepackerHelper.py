
import logging
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
#import MySQLdb
import ConfigDB
import DbsLink
import RepackerIterator
import os
import sys
import traceback
import pickle
from ProdCommon.MCPayloads.Tier0WorkflowMaker import Tier0WorkflowMaker
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec


from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.JobSpec import JobSpec




class RepackerHelper:

    def __init__(self, args):
        self.args=args
        self.workflow_by_ds={}
        self.loader = CMSSWAPILoader(self.args['CMSSW_Arch'],
                                     self.args['CMSSW_Ver'],
                                     self.args['CMSSW_Dir']) # XXX
        self.loader.load()
        import FWCore.ParameterSet.parseConfig as ConfigParser

        cfg = self.args['RepackerCfgTmpl']
        logging.info("Parsing template file [%s]"%cfg)
        self.cmsCfg = ConfigParser.parseCfgFile(cfg)
        logging.info("Done parsing template file [%s]"%cfg)


    def createJobSpec(self, ds_key, tags, pfn_list, lumi_data):
        rep_iter = self.workflow_by_ds[ds_key]
        job_spec_path,job_spec_file,job_spec = rep_iter(pfn_list)
        #print "CFG_1",job_spec.payload.cfgInterface.rawCfg
        #print "CFG_2",job_spec.payload.configuration

        self._setLumiData(job_spec_file,job_spec,lumi_data)
        
        rep_iter.save(rep_iter.workingDir)

        return job_spec_path
        


    def _setLumiData(self,job_spec_file,job_spec,lumi_data):
        # Set lumi data here
        print "Set LumiData",lumi_data
        cfgInstance = pickle.loads(job_spec.payload.cfgInterface.rawCfg)
        #print "DUMP:",cfgInstance.dumpConfig()
        #print "PRODUCERS:",cfgInstance.producers_()
        # Get producers list (lumi module is EDProducer)
        producers_list=cfgInstance.producers_()
        mod_lumi=producers_list['lumi']
        print "LumiModule",mod_lumi.parameterNames_()
        # bla-bla

        # save spec after update
        job_spec.save(job_spec_file)


    def prepareWorkflow(self, run_number, primary_ds_name, processed_ds_name):
        logging.debug("prepareWorkflow(%s, %s, %s)" % (run_number, primary_ds_name, processed_ds_name))
        workflowHash = "%s-%s-%s" % (run_number, primary_ds_name, processed_ds_name)

        workflowDir = os.path.join(self.args['ComponentDir'], workflowHash)
        workflowFile = os.path.join(workflowDir, "%s-Workflow.xml" % workflowHash)
        
        if os.path.exists(workflowFile):
            logging.info("Workflow Spec exists: Reloading state...")
            logging.debug("Loading: %s" % workflowFile)
            repacker_iter = self.workflow_by_ds[workflowHash]
            repacker_iter.load(workflowDir)
        else:
            logging.info("Creating new workflow: %s" % workflowHash)
            if not os.path.exists(workflowDir):
                os.makedirs(workflowDir)
            spec = self._createNewWorkflow(workflowFile, primary_ds_name, processed_ds_name, run_number)

            logging.info("Creating new iterator: %s" % workflowHash)
            repacker_iter = RepackerIterator.RepackerIterator(workflowFile, workflowDir)
            self.workflow_by_ds[workflowHash] = repacker_iter
            repacker_iter.save(workflowDir)
        return (workflowFile,workflowHash)


    def _createNewWorkflow(self, filename, primaryDS, procDS, runNumber):
        """
        _createNewWorkflow_
        
        Create new workflow spec
        For the dataset/run info provided
        and save the file in the location provided.
        Returns the new spec instance

        """
        requestId = str(runNumber)
        channel = primaryDS
        group = self.args['JobGroup']
        label = procDS
        
        cfgWrapper = CMSSWConfig()
        cfgWrapper.originalCfg = self.cmsCfg.dumpConfig()
        cfgWrapper.loadConfiguration(self.cmsCfg)
        cfgInt = cfgWrapper.loadConfiguration(self.cmsCfg)
        cfgInt.validateForProduction()

        wfmaker =Tier0WorkflowMaker(requestId, channel, label)
        wfmaker.setRunNumber(runNumber)
        wfmaker.changeCategory("data");
        wfmaker.setCMSSWVersion(self.args['CMSSW_Ver'])
        wfmaker.setPhysicsGroup(group)
        wfmaker.setConfiguration(cfgWrapper, Type = "instance")  #// - According to Dave....
        wfmaker.setPSetHash("NA")
        wfmaker.addInputDataset("/%s/%s/RAW" % (primaryDS , procDS))

        spec = wfmaker.makeWorkflow()
        spec.save(filename)
        return spec
