#!/usr/bin/env python
"""
_RepackerIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow based on some set of input
parameters defining the input LFNs to a repacker job


Also make this object persistable so that it is crash resistant.

"""

import os
import logging


from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import createUnmergedLFNs
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdAgentCore.Configuration import loadProdAgentConfiguration


from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile


class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance.

    """
    def __init__(self):
	dict.__init__(self)


    def __call__(self, payloadNode):
	if payloadNode.cfgInterface != None:
	    generator = CfgGenerator(payloadNode.cfgInterface, False,payloadNode.applicationControls)
	    self[payloadNode.name] = generator
	    return

	if payloadNode.configuration in ("", None):
	    #  //
	    # // Isnt a config file
	    #//
	    return
	try:
	    generator = CfgGenerator(payloadNode.configuration, True,payloadNode.applicationControls)
	    self[payloadNode.name] = generator
	except StandardError, ex:
	    #  //
	    # // Cant read config file => not a config file
	    #//
	    return




class RepackerIterator:
    """
    _RepackerIterator_

    Working from a WorkflowSpec template, generate
    concrete jobs from it, keeping in-memory history
    and persistant backup in the working directory

    """
    def __init__(self, workflowSpecFile, workingDir):
	self.workflow = workflowSpecFile
	self.workingDir = workingDir
	self.workflowSpec = WorkflowSpec()
	self.workflowSpec.load(workflowSpecFile)
	self.count = 0

	#  //
	# // Temp container to manage per job LFNs
	#//
	self.inputLFNs = []

	#  //
	# // Keep track of each job spec file until it is passed
	#//  to the main PA so that we can clean up.
	self.ownedJobSpecs = {}

	#  //
	# // The generators map keeps the CfgGenerators for creating
	#//  the jobs
	self.generators = GeneratorMaker()
	self.generators(self.workflowSpec.payload)


	#  //
	# // Cache Area for JobSpecs
	#//
	self.specCache = os.path.join(self.workingDir,"%s-Cache" %self.workflowSpec.workflowName())
	if not os.path.exists(self.specCache):
	    os.makedirs(self.specCache)


    def __call__(self, inputLFNs):
	"""
	_operator()_

	When called generate a new concrete job payload from the
	generic workflow and return it.
	The JobDef should be a JobDefinition with the input details
	including LFNs and event ranges etc.

	"""
	JSPath,JSFile,JSObj = self.createJobSpec(inputLFNs)
	self.count += 1
	return (JSPath,JSFile,JSObj)




    def inputDataset(self):
	"""
	_inputDataset_

	Extract the input Dataset from this workflow

	"""
	topNode = self.workflowSpec.payload
	try:
	    inputDataset = topNode._InputDatasets[-1]
	except StandardError, ex:
	    msg = "Error extracting input dataset from Workflow:\n"
	    msg += str(ex)
	    logging.error(msg)
	    return None

	return inputDataset.name()



    def createJobSpec(self, inputLFNs):
	"""
	_createJobSpec_

	Load the WorkflowSpec object and generate a JobSpec from it

	"""

	jobSpec = self.workflowSpec.createJobSpec()
	jobName = "%s-%s" % (self.workflowSpec.workflowName(),self.count,)
	self.currentJob = jobName
	self.inputLFNs = inputLFNs
	jobSpec.setJobName(jobName)
	jobSpec.setJobType("Processing")
	jobSpec.parameters['RunNumber'] = self.count


	jobSpec.payload.operate(self.generateJobConfig)

	#  //
	# // keep track of the file location
	#//
	specCacheDir =  os.path.join(self.specCache, str(self.count // 1000).zfill(4))
	if not os.path.exists(specCacheDir):
	    os.makedirs(specCacheDir)
	jobSpecFile = os.path.join(specCacheDir,"%s-JobSpec.xml" % jobName)
	self.ownedJobSpecs[jobName] = jobSpecFile

	#  //
	# // generate LFNs for output modules
	#//  (This may have to change for t0, not sure yet...)
	createUnmergedLFNs(jobSpec)

	jobSpec.save(jobSpecFile)

	return ("file://%s" % jobSpecFile,jobSpecFile,jobSpec)




    def generateJobConfig(self, jobSpecNode):
	"""
	_generateJobConfig_

	Operator to act on a JobSpecNode tree to convert the template
	config file into a JobSpecific Config File

	"""
	if jobSpecNode.name not in self.generators.keys():
	    return

	generator = self.generators[jobSpecNode.name]


	#  //
	# // Pass the list of input files to the source
	#//  configure the job to read all the events
	args = {'fileNames' : self.inputLFNs,'maxEvents' : -1,}

	jobCfg = generator(self.currentJob, **args)

	#for outModName, outModData in jobCfg.outputModules.items():
	    #logging.debug("outModName = %s, outModData = %s" % (outModName, outModData))

	jobSpecNode.cfgInterface = jobCfg
	return 



    def removeSpec(self, jobSpecId):
	"""
	_removeSpec_

	Remove a Spec file when it has been successfully injected

	"""
	if jobSpecId not in self.ownedJobSpecs.keys():
	    return

	logging.info("Removing JobSpec For: %s" % jobSpecId)
	filename = self.ownedJobSpecs[jobSpecId]
	if os.path.exists(filename):
	    os.remove(filename)
	    del self.ownedJobSpecs[jobSpecId]
	return



    def save(self, directory):
	"""
	_save_

	Save details of this object to the dir provided using
	the basename of the workflow file

	"""
	doc = IMProvDoc("RepackerIterator")
	node = IMProvNode(self.workflowSpec.workflowName())
	doc.addNode(node)

	node.addNode(IMProvNode("Run", None, Value = str(self.count)))

	specs = IMProvNode("JobSpecs")
	node.addNode(specs)
	for key, val in self.ownedJobSpecs.items():
	    specs.addNode(IMProvNode("JobSpec", val, ID = key))

	fname = os.path.join(directory,"%s-Persist.xml" % self.workflowSpec.workflowName())
	handle = open(fname, 'w')
	handle.write(doc.makeDOMDocument().toprettyxml())
	handle.close()

	return


    def load(self, directory):
	"""
	_load_

	For this instance, search for a params file in the dir provided
	using the workflow name in this instance, and if present, load its
	settings

	"""
	fname = os.path.join(directory,"%s-Persist.xml" % self.workflowSpec.workflowName())

	node = loadIMProvFile(fname)

	qbase = "/RepackerIterator/%s" % self.workflowSpec.workflowName()

	runQ = IMProvQuery("%s/Run[attribute(\"Value\")]" % qbase)

	runVal = int(runQ(node)[-1])
	self.count = runVal
	specQ = IMProvQuery("%s/JobSpecs/*" % qbase)
	specNodes = specQ(node)
	for specNode in specNodes:
	    specId = str(specNode.attrs['ID'])
	    specFile = str(specNode.chardata).strip()
	    self.ownedJobSpecs[specId] = specFile
	return



def readStringFromFile(filename):
    """
    _readStringFromFile_

    util to extract file content as a string

    """
    if not os.path.exists(filename):
	return None
    content = file(filename).read()
    content = content.strip()
    return content

