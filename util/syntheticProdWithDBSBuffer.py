#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
_syntheticProduction_

Unittest and benchmarking data generator for the Production system.
Given a production workflow spec, generate a bunch of job spec files,
create job reports for them, simulate merging them and create merge reports


"""
__revision__ = "$Id: syntheticProduction.py,v 1.1 2008/06/06 19:28:30 evansde Exp $"
__version__ = "$Revision: 1.1 $"


import sys
import os
import getopt

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.UUID import makeUUID
from ProdCommon.MCPayloads.MergeTools import createMergeJobWorkflow
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

from ProdCommon.JobFactory.RequestJobFactory import RequestJobFactory
from JobEmulator.JobReportPlugins.EmulatorReportPlugin import EmulatorReportPlugin

import commands
import logging
import os
import threading

from WMComponent.DBSUpload.DBSUpload import DBSUpload
from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

class DBSBufferInterface:
    _setup_done = False
    _teardown = False
    _maxMessage = 1000

    def setUp(self):
        """
        setup for test.
        """

	print "Assuming that DBSBuffer database is already created......!!!!!!!!!"


        if not DBSBufferInterface._setup_done:
                logging.basicConfig(level=logging.NOTSET,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='%s.log' % __file__,
                        filemode='w')

                myThread = threading.currentThread()
                myThread.logger = logging.getLogger('DBSBufferInterface')
                myThread.dialect = 'MySQL'

                options = {}
                options['unix_socket'] = os.getenv("DBSOCK")
                dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                        options)

                myThread.dbi = dbFactory.connect()
                myThread.transaction = Transaction(myThread.dbi)
                #myThread.transaction.begin()
                #myThread.transaction.commit()
                DBSBufferInterface._setup_done = True

    def uploadWorkflowSpec(self, wkflo):
        """
        Mimics creation of component and handles messages.
        """

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSUpload/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Upload"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")
        
        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql' 
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        testDBSUpload = DBSUpload(config)
        
        testDBSUpload.prepareToStart()
	
        testDBSUpload.handleMessage('NewWorkflow', wkflo)

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        DBSBufferInterface._teardown = True

    def bufferFWJR(self, fwjr):
        """
        Mimics creation of component and handles JobSuccess messages.
        """

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSBuffer/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Buffer"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql'
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('DBSBufferInterface')
        myThread.dialect = 'MySQL'

        options = {}
        options['unix_socket'] = os.getenv("DBSOCK")
        dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

        testDBSBuffer = DBSBuffer(config)
        testDBSBuffer.prepareToStart()

        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

	testDBSBuffer.handleMessage('JobSuccess', fwjr)
	
	while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)
        DBSBufferInterface._teardown = True

    def upload(self):

        """
        Mimics creation of component and handles come messages.

	Upload just 10 files for testing ONLY
        """

        #return True

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSUpload/DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Upload"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql'
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        testDBSUpload = DBSUpload(config)
        testDBSUpload.prepareToStart()

        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # for testing purposes we use this method instead of the 
        # StartComponent one.

        testDBSUpload.handleMessage('BufferSuccess', \
                                'NoPayLoad')

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        DBSBufferInterface._teardown = True


#  //
# // Command line args
#//
valid = [ 'prod-workflow=', 'prod-events=',
          'files-per-merge=', 'working-dir=']


usage = "Usage: syntheticProduction.py  --prod-workflow=<workflow spec>\n"
usage += "                         --prod-events=<event total>\n"
usage += "                         --files-per-merge=<num files per merge>\n"
usage += "                         --working-dir=<path to scratch space>\n"


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


workflowFile = None
workingDir = None
productionEvents = 1000
filesPerMergeJob = 10

for opt, arg in opts:
    if opt == "--prod-workflow":
        workflowFile = arg

    if opt == "--working-dir":
        workingDir = arg

    if opt == "--files-per-merge":
        filesPerMerge = int(arg)

    if opt == "--prod-events":
        totalEvents = int(arg)


if workflowFile == None:
    msg = "--prod-workflow not set"
    raise RuntimeError, msg
if workingDir == None:
    msg = "--working-dir not set"
    raise RuntimeError, msg


#SETUP DBSBuffer
print "Setting up DBS Buffer...."
dbsbuffer=DBSBufferInterface()
dbsbuffer.setUp()

#  //
# // Script wide objects
#//
workflowSpec = WorkflowSpec()
workflowSpec.load(workflowFile)
##DBS Buffer Hook, Upload Primary/Proc/Algo from the the Workflow Spec file

print "Calling on DBS Buffer to do JobSpec file"
dbsbuffer.uploadWorkflowSpec(workflowFile)


productionDir = "%s/production" % workingDir
mergeProdDir = "%s/production-merge" % workingDir

if not os.path.exists(productionDir):
    os.makedirs(productionDir)
if not os.path.exists(mergeProdDir):
    os.makedirs(mergeProdDir)

mergeProdSpecs = createMergeJobWorkflow(workflowSpec)
prodFactory = RequestJobFactory(workflowSpec, productionDir, productionEvents)

for mergeDS, mergeSpec in mergeProdSpecs.items():
    mrgSpecFile = "%s/%s.xml" % (mergeProdDir, mergeDS.replace("/", "_"))
    mergeSpec.save(mrgSpecFile)


#  //
# // make production job definitions
#//
prodJobs = prodFactory()

prodToMergeDatasets = {}
prodFiles = {}
for prodDataset in workflowSpec.outputDatasets():
    dsName = prodDataset.name()
    prodFiles[dsName] = set()
    prodToMergeDatasets[dsName] = mergeProdSpecs[dsName]



emulator2 = EmulatorReportPlugin()

wnInfo = {
    "SiteName" : "TN_SITE_CH",
    "HostID"   : "host" ,
    "HostName" : "workernode.element.edu",
    "se-name"  : "storage.element.edu",
    "ce-name"  : "compute.element.edu",
}

for prodJob in prodJobs:
    jobSpec = JobSpec()
    jobSpec.load(prodJob['JobSpecFile'])
    jobReport = "%s/%s-JobReport.xml" % (
        productionDir, jobSpec.payload.jobName)
    print jobReport
    repInstance = emulator2.createSuccessReport(jobSpec, wnInfo, jobReport)
    
    for fileinfo in repInstance.files:
        lfn = fileinfo['LFN']
        for dataset in  [ x.name() for x in fileinfo.dataset ] :
            prodFiles[dataset].add(lfn)
    print "Buffering Job Report File"
    dbsbuffer.bufferFWJR(jobReport)

print "At this point we can upload the Buffered FWJRs to DBS..........."
dbsbuffer.upload()  


class MergeMaker:

    def __init__(self, mergeSpec, mergeWorkDir):
        self.spec = mergeSpec
        self.dir = mergeWorkDir
        self.count = 0
        self.dataset = self.spec.outputDatasets()[0]
        self.spec.payload.cfgInterface = CMSSWConfig()
        cfgInt = self.spec.payload.cfgInterface
        cfgInt.sourceType = "PoolSource"
        cfgInt.maxEvents['input'] = -1
        cfgInt.configMetadata['name'] = "Merge"
        cfgInt.configMetadata['version'] = "AutoGenerated"
        cfgInt.configMetadata['annotation'] = "AutoGenerated For Scale test"

        outputModule = cfgInt.getOutputModule("Merged")
        outputModule["catalog"] = '%s-Catalog.xml' % outputModule['Name']
        outputModule["primaryDataset"] = self.dataset['PrimaryDataset']
        outputModule["processedDataset"] = self.dataset['ProcessedDataset']
        outputModule["dataTier"] = "RAW"
        outputModule['LFNBase'] = self.spec.parameters['MergedLFNBase']



    def __call__(self, *fileList):

        jobSpec = self.spec.createJobSpec()

        jobId = "%s-%s" % (self.spec.workflowName(), self.count)

        jobSpec.setJobName(jobId)
        jobSpec.setJobType("Merge")



        jobSpec.addWhitelistSite("storage.element.edu")

        # get PSet
        cfg = jobSpec.payload.cfgInterface

        # set output module

        #print jobSpec.payload




        # set output file name

        prim = self.dataset['PrimaryDataset']
        tier = self.dataset['DataTier']
        lastBit = self.dataset['ProcessedDataset']

        acqEra = None
        #if .has_key("AcquisitionEra"):
        acqEra = jobSpec.parameters.get("AcquisitionEra", None)

        # compute LFN group based on merge jobs counter
        group = str(self.count // 1000).zfill(4)
        jobSpec.parameters['RunNumber'] = self.spec.workflowRunNumber()
        remainingBits = lastBit
        if acqEra != None:
            thingtoStrip = "%s_" % acqEra
            mypieces = lastBit.split(thingtoStrip, 1)
            if len(mypieces) > 1:
                remainingBits = mypieces[1].split("-unmerged", 1)[0]
            else:
                remainingBits=lastBit


        outModule = cfg.outputModules['Merged']
        lfnBase = outModule['LFNBase']
        extendedlfnBase = os.path.join(lfnBase, prim, tier, remainingBits,
                                       group)
        baseFileName = "%s.root" % makeUUID()

        outModule['fileName'] = baseFileName
        outModule['logicalFileName'] = os.path.join(extendedlfnBase,
                                                    baseFileName)

        # set output catalog
        outModule['catalog'] = "%s-merge.xml" % jobId

        # set input module


        # get input file names (expects a trivial catalog on site)
        cfg.inputFiles = ["%s" % fileName for fileName in fileList]


        # target file name
        mergeJobSpecFile = "%s/%s-spec.xml" % (
            self.dir, jobId)

        # save job specification
        jobSpec.save(mergeJobSpecFile)
        self.count += 1
        return jobSpec


for dataset, mergeWorkflow in mergeProdSpecs.items():
    maker = MergeMaker(mergeWorkflow, mergeProdDir)
    mrgEmulator = EmulatorReportPlugin()
    inputFiles = list(prodFiles[dataset])
    mergejobs = [inputFiles[i:i + filesPerMergeJob]
                 for i  in range(0, len(inputFiles), filesPerMergeJob)]
    for mergejob in mergejobs:
        mergeSpec = maker(*mergejob)
        mrgReportFile = "%s/%s-Merge-JobReport.xml" % (
            mergeProdDir, mergeSpec.payload.jobName)
        mrgEmulator.createSuccessReport(mergeSpec, wnInfo, mrgReportFile)


