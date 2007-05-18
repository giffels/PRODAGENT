#!/usr/bin/env python
"""
_RepackerInjectorComponent_

Component for generating Repacker JobSpecs

"""



__version__ = "$Revision$"
__revision__ = "$Id$"
__author__ = "kss"


import logging
from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
#import MySQLdb
import ConfigDB
import DbsLink
import RepackerIterator
import os
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker

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

	Poll DBS and generate repacker job specs

	"""
	def __init__(self, **args):
		self.watching_runs=[]
		self.watching_ds={}
		self.workflow_by_ds={}
		self.args = {}
		#  //
		# // Set default args
		#//
		self.args['PollInterval'] = "00:00:15"
		#  //
		# // Override defaults with those from ProdAgentConfig
		#//
		self.args.update(args)
		#print "DB=[%s] h=[%s] p=[%s] u=[%s]" % (self.args["DbsDbName"],self.args["DbsDbHost"],self.args["DbsDbPort"],self.args["DbsDbUser"])
		LoggingUtils.installLogHandler(self)
		msg = "RepackerInjector Started:\n"
		msg += " PollInterval: %s\n" % self.args['PollInterval']
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
		
		if message == "RepackerInjector:PollLoop":
			#  //
			# // this triggers a poll
			#//
			self.pollLoop()
			return
			
		if message == "RepackerInjector:StartNewRun":
			#  //
			# // On demand action
			#//
			self.doStartNewRun(payload)
			return


	def pollLoop(self):
		"""
		_pollLoop_

		Example of how to make the component loop periodically

		"""
		#print "pollLoop"
		logging.info("Poll Loop invoked...")
	
		self.do_dbsPoll()

		#  //
		# //  When you have done the periodic task, publish poll trigger
		#//   to self, with a delay set by the PollInterval arg.
		self.ms.publish("RepackerInjector:PollLoop", "",
		self.args['PollInterval'])
		self.ms.commit()
		return



	def doStartNewRun(self, payload):
		"""
		Expects run number and source dataset name in the payload in the form "run_number primary_ds_name source_ds_name"
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
		source_ds_name=items[2]
		logging.info("RunNumber %d PDS [%s] SDS [%s]"%(run_number,primary_ds_name,source_ds_name))
		if(run_number in self.watching_runs):
			""" Already watching """
			logging.info("Already watching run %d, ignoring request"%(run_number,))
			return
		""" Create new workflow spec """
		requestId="repacker_run_"+`run_number`
		channel="Channel1" # XXX
		group=self.args['JobGroup']
		label=self.args['JobLabel']
		cfg=self.args['RepackerCfgTmpl']
		loader = CMSSWAPILoader(self.args['CMSSW_Arch'],self.args['CMSSW_Ver'],self.args['CMSSW_Dir']) # XXX
		loader.load()
		import FWCore.ParameterSet.parseConfig as ConfigParser
		cmsCfg = ConfigParser.parseCfgFile(cfg)
		cfgWrapper = CMSSWConfig()
		cfgWrapper.loadConfiguration(cmsCfg)
		cfgWrapper.originalCfg = file(cfg).read()
		loader.unload()
		
		wfmaker=WorkflowMaker(requestId, channel, label)
		wfmaker.setCMSSWVersion(self.args['CMSSW_Ver'])
		wfmaker.setPhysicsGroup(group)
#		wfmaker.setConfiguration(cfgWrapper.pack(), Format = "CMSSWConfig", Type = "string")
		wfmaker.setConfiguration(cfgWrapper, Type = "instance")
		wfmaker.setPSetHash("MadeUpHashForTesting")
		
		spec=wfmaker.makeWorkflow()
		ds_key=primary_ds_name+':'+source_ds_name
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
		
		self.watching_ds[run_number]=(primary_ds_name,source_ds_name)
		self.watching_runs.append(run_number)
		logging.info("Added to watching list: RunNumber %d PDS [%s] SDS [%s]"%(run_number,primary_ds_name,source_ds_name))
		return




	def do_dbsPoll(self):
		if(not self.watching_runs):
			logging.debug("No runnum to watch")
			return

#		dbslink=DbsLink.DbsLink(db=self.args["DbsDbName"],host=self.args["DbsDbHost"],user=self.args["DbsDbUser"],passwd=self.args["DbsDbPasswd"])
		dbslink=DbsLink.DbsLink(url=self.args["DbsUrl"],level=self.args["DbsLevel"])

		for i in self.watching_runs:
			#print "Polling run",i
			pri_ds,pro_ds=self.watching_ds[i]
			#print pri_ds,pro_ds
			file_res=dbslink.poll_for_files(pri_ds,pro_ds)
			for i in file_res:
				lfn,tags=i
				res_job_error=self.submit_job(lfn,tags,pri_ds,pro_ds)
				if(not res_job_error):
					dbslink.setFileStatus(lfn,"submitted")
					dbslink.commit()
				
		dbslink.close()


	def submit_job(self,lfn,tags,pri_ds,pro_ds):
		logging.info("Creating job for file [%s] tags %s"%(lfn,str(tags)))
		ds_key=pri_ds+':'+pro_ds
		rep_iter=self.workflow_by_ds[ds_key]
#		job_spec=spec.createJobSpec()
		job_spec=rep_iter(lfn)
		
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

		self.ms.subscribeTo("RepackerInjector:PollLoop")
		self.ms.subscribeTo("RepackerInjector:StartNewRun")

		# generate first polling cycle
		self.ms.remove("RepackerInjector:PollLoop")
		self.ms.publish("RepackerInjector:PollLoop", "")
		self.ms.commit()

		# wait for messages
		while True:
			type, payload = self.ms.get()
			self.ms.commit()
			logging.debug("RepackerInjector: %s, %s" % (type, payload))
			#print "Message"
			self.__call__(type, payload)

		
