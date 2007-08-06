"""
_RepackerHelper_

Creates and manipulates Workflow and Job Specs for teh RepackerInjectorComponent

"""


__version__ = "$Revision: 1.7 $"
__revision__ = "$Id: RepackerHelper.py,v 1.7 2007/07/24 14:26:33 hufnagel Exp $"
__author__ = "kss"


import logging
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
import RepackerIterator
import os
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


    def createJobSpec(self, ds_key, tags, lfnList, lumiList):

        rep_iter = self.workflow_by_ds[ds_key]
        job_spec_path,job_spec_file,job_spec = rep_iter(lfnList)

        if(self.args.has_key('LumiServerUrl')):
            from LumiServerLink import getLumiServerLink
            lslink=getLumiServerLink(self.args)
            lslink.setLumiData(job_spec_file,job_spec,lumiList)

        rep_iter.save(rep_iter.workingDir)
        return job_spec_path
        




    def prepareWorkflow(self, run_number, primary_ds_name, processed_ds_name):
        logging.debug("prepareWorkflow(%s, %s, %s)" % (run_number, primary_ds_name, processed_ds_name))
        workflowHash = "%s-%s-%s" % (run_number, primary_ds_name, processed_ds_name)

        workflowDir = os.path.join(self.args['ComponentDir'], workflowHash)
        workflowFile = os.path.join(workflowDir, "%s-Workflow.xml" % workflowHash)

        createdNewWorkflow = False

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
            createdNewWorkflow = True

        return (workflowFile,workflowHash,createdNewWorkflow)


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

        wfmaker = Tier0WorkflowMaker(requestId, channel, label)
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
