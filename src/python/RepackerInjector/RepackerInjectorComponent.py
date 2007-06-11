#!/usr/bin/env python
"""
_RepackerInjectorComponent_

Component for generating Repacker JobSpecs

"""



__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: RepackerInjectorComponent.py,v 1.5 2007/06/06 14:18:54 hufnagel Exp $"
__author__ = "kss"


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
from ProdCommon.MCPayloads.Tier0WorkflowMaker import Tier0WorkflowMaker

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from ProdCommon.MCPayloads.JobSpec import JobSpec


"""
!!! Every 'print' statement line ends with '# XXX' comment !!!
(means 'print's will be replaced with 'log()'s)
"""


class RepackerInjectorComponent:
	"""
	_RepackerInjectorComponent_

	Query DBS and generate repacker job specs

	"""
	def __init__(self, **args):
		self.workflow_by_ds={}
		self.args = {}
		#  //
		# // Set default args
		#//

		#  //
		# // Override defaults with those from ProdAgentConfig
		#//
		self.args.update(args)
		#print "DB=[%s] h=[%s] p=[%s] u=[%s]" % (self.args["DbsDbName"],self.args["DbsDbHost"],self.args["DbsDbPort"],self.args["DbsDbUser"])
		LoggingUtils.installLogHandler(self)
		msg = "RepackerInjector Started:\n"
		logging.info(msg)
		logging.info("args %s"%str(args))
		logging.info("URL=[%s] level=[%s]" % (self.args["DbsUrl"],self.args["DbsLevel"]))


	def __call__(self, message, payload):
		"""
		_operator()_

		Define responses to messages

		"""
		msg = "Recieved Event: %s " % message
		msg += "Payload: %s" % payload
		logging.debug(msg)

		#  //
		# // All components respond to standard debugging level control
		#//
		if message == "RepackerInjector:StartDebug":
			logging.getLogger().setLevel(logging.DEBUG)
			return
		if message == "RepackerInjector:EndDebug":
			logging.getLogger().setLevel(logging.INFO)
			return

		#  //
		# // Component Specific actions.
		#//
		
		if message == "RepackerInjector:StartNewRun":
			#  //
			# // On demand action
			#//
			self.doStartNewRun(payload)
			return


	def doStartNewRun(self, payload):
		"""
		Expects run number and source dataset name in the payload in the form "run_number primary_ds_name processed_ds_name"
		"""
		logging.info("StartNewRun(%s)" % payload)
		items=payload.split(" ")
		if(len(items)!=3):
			logging.error("StartNewRun(%s) - bad payload format" % payload)
			return
		run_number=-1
		try:
			run_number=int(items[0])
		except ValueError:
			logging.error("StartNewRun - bad runnumber [%s]" % items[0])
			return
		primary_ds_name=items[1]
		processed_ds_name=items[2]
		logging.info("RunNumber %d PDS [%s] SDS [%s]"%(run_number,primary_ds_name,processed_ds_name))

		""" Create new workflow spec """
		requestId=str(run_number)
		channel=primary_ds_name
		group=self.args['JobGroup']
		#label=self.args['JobLabel']
                label=processed_ds_name
		cfg=self.args['RepackerCfgTmpl']
		loader = CMSSWAPILoader(self.args['CMSSW_Arch'],self.args['CMSSW_Ver'],self.args['CMSSW_Dir']) # XXX
		loader.load()
		import FWCore.ParameterSet.parseConfig as ConfigParser
		cmsCfg = ConfigParser.parseCfgFile(cfg)
		cfgWrapper = CMSSWConfig()
                cfgWrapper.originalCfg = file(cfg).read()
                cfgWrapper.loadConfiguration(cmsCfg)
                cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
                cfgInt.validateForProduction()

		wfmaker=Tier0WorkflowMaker(requestId, channel, label)
                wfmaker.setRunNumber(run_number)
                wfmaker.changeCategory("data");
		wfmaker.setCMSSWVersion(self.args['CMSSW_Ver'])
		wfmaker.setPhysicsGroup(group)
#		wfmaker.setConfiguration(cfgWrapper.pack(), Format = "CMSSWConfig", Type = "string")
		wfmaker.setConfiguration(cfgWrapper, Type = "instance")
		wfmaker.setPSetHash("NA")
                wfmaker.addInputDataset("/%s/%s/RAW" % (primary_ds_name ,
                                                        processed_ds_name)
                                        )

                spec=wfmaker.makeWorkflow()
		ds_key=processed_ds_name + '-' + primary_ds_name + '-' + str(run_number)
		compdir=self.args['ComponentDir']
		workdir=compdir+'/'+ds_key
		if(ds_key in os.listdir(compdir)):
			logging.info("Dir [%s] already exists !!!"%(workdir,))
		else:
			os.mkdir(workdir)
		#print spec.outputDatasets()
#		print spec.outputDatasetsWithPSet()
		wfspecfile=workdir+"/workflow.spec"
		spec.save(wfspecfile)

		self.ms.publish("NewWorkflow",wfspecfile)
		self.ms.publish("NewDataset",wfspecfile)
		self.ms.commit()
		repacker_iter=RepackerIterator.RepackerIterator(wfspecfile,workdir)
		self.workflow_by_ds[ds_key]=repacker_iter
		
                # HACK, process all files in the datasets specified for the run specified
                # good enough for glbal running May data, not sufficient for live system

                #
                # Final solution is not supposed to need run number.
                # The run number is discovered in the DBS query and
                # overridden in the workflow before the job is submitted.
                #
                
                dbslink=DbsLink.DbsLink(url=self.args["DbsUrl"],level=self.args["DbsLevel"])

                file_res=dbslink.poll_for_files(primary_ds_name,processed_ds_name,run_number)

                for i in file_res:
                    lfn,tags=i
                    logging.info("Found file %s" % lfn)
                    res_job_error=self.submit_job(lfn,tags,primary_ds_name,processed_ds_name,ds_key)
                    if(not res_job_error):
                        dbslink.setFileStatus(lfn,"submitted")
                        dbslink.commit()
                        logging.info("Submitted job for %s" % lfn)
				
		dbslink.close()
                
		return


	def submit_job(self,lfn,tags,pri_ds,pro_ds,ds_key):
		logging.info("Creating job for file [%s] tags %s"%(lfn,str(tags)))
                #
                # FIXME: NewStreamerEventStreamFileReader cannot use LFN
                #        either fix that or resolve PFN here via TFC
                #
                pfn = 'rfio:/?path=/castor/cern.ch/cms' + lfn
                logging.info("Creating job for file [%s] tags %s"%(pfn,str(tags)))
		rep_iter=self.workflow_by_ds[ds_key]
#		job_spec=spec.createJobSpec()
		job_spec=rep_iter([pfn])
		
		self.ms.publish("CreateJob",job_spec)
		self.ms.commit()
		logging.info("CreateJob signal sent, js [%s]"%(job_spec,))
		return 0

	

	def startComponent(self):
		"""
		_startComponent_

		Start up the component and define the messages that it subscribes to

		"""

 
		#print "Started"
		# create message service
		self.ms = MessageService()
		# register this component
		self.ms.registerAs("RepackerInjector")

		# subscribe to messages
		self.ms.subscribeTo("RepackerInjector:StartDebug")
		self.ms.subscribeTo("RepackerInjector:EndDebug")

		self.ms.subscribeTo("RepackerInjector:StartNewRun")

		self.ms.commit()

		# wait for messages
		while True:
			type, payload = self.ms.get()
			self.ms.commit()
			logging.debug("RepackerInjector: %s, %s" % (type, payload))
			#print "Message"
			self.__call__(type, payload)

		
