
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
        if(len(lumi_data)<=1):
            logging.info("Insufficient lumi data - ignoring")
            return
        # Set lumi data here
        #print "Set LumiData",lumi_data
        cfgInstance = pickle.loads(job_spec.payload.cfgInterface.rawCfg)
        #print "PRODUCERS:",cfgInstance.producers_()
        # Get producers list (lumi module is EDProducer)
        producers_list=cfgInstance.producers_()
        mod_lumi=producers_list['lumiProducer']
        #print "LumiModule",mod_lumi.parameterNames_(),dir(mod_lumi)
        #Get template pset for the lumi module
        pset_name=mod_lumi.parameterNames_()[0]
        pset=getattr(mod_lumi,pset_name)

        #Clean the template pset name
        delattr(mod_lumi,pset_name)
        #print "LumiModule2",mod_lumi.parameterNames_()

        #Create the real PSet name"
        pset_name="LS"+str(lumi_data['lsnumber'])
        pset.setLabel(pset_name)
        #Set parameters
        pset.avginslumi=lumi_data['avginslumi']
        pset.avginslumierr=lumi_data['avginslumierr']
        pset.lumisecqual=int(lumi_data['lumisecqual'])
        pset.deadfrac=lumi_data['deadfrac']
        pset.lsnumber=int(lumi_data['lsnumber'])

        pset.lumietsum=lumi_data['det_et_sum']
        pset.lumietsumerr=lumi_data['det_et_err']
        pset.lumietsumqual=lumi_data['det_et_qua']
        pset.lumiocc=lumi_data['det_occ_sum']
        pset.lumioccerr=lumi_data['det_occ_err']
        pset.lumioccqual=lumi_data['det_occ_qua']
        
        #Insert the pset into the lumi module
        setattr(mod_lumi,pset_name,pset)
        
        # bla-bla
        #print "DUMP:",cfgInstance.dumpConfig()

        # save spec after update
        job_spec.save(job_spec_file)
        return


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
