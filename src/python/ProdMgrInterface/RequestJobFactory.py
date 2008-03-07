


from ProdCommon.JobFactory.RequestJobFactory import RequestJobFactory as BaseJobFactory
from ProdCommon.JobFactory.RequestJobFactory import GeneratorMaker
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker

from ProdAgent.WorkflowEntities import Aux

import logging
import math
import os

class RequestJobFactory(BaseJobFactory):

    def __init__(self, workflowSpec, workingDir, totalEvents, **args):
        self.pileupDatasets = {}
        self.sites = args.get("Sites", [] )


        logging.debug("Initialize RequestJobFactory subclass")


    def init(self):
        self.generators = GeneratorMaker()
        self.workflowSpec.payload.operate(self.generators)
        #  //
        # // Cache Area for JobSpecs
        #//
        self.specCache = os.path.join(
            self.workingDir,
            "%s-Cache" %self.workflowSpec.workflowName())
        if not os.path.exists(self.specCache):
            os.makedirs(self.specCache)


    def __call__(self):
        """
        _operator()_

        When called generate a new concrete job payload from the
        generic workflow and return it.

        """

        self.loadPileupDatasets()

        self.eventsPerJob = int(math.ceil(float(self.totalEvents)/float(len(self.job_run_numbers))))
        self.currentEvent = self.start_event
        result = []

        for i in xrange(0,len(self.job_run_numbers)):
            self.count = self.job_run_numbers[i]

            self.currentEvent += self.eventsPerJob
            if((self.currentEvent)>(self.start_event+int(self.totalEvents))):
                self.eventsPerJob=self.currentEvent - self.start_event - self.totalEvents 


            jobSpecFile = self.createJobSpec()
            result.append({'id':self.currentJob,'spec':jobSpecFile,'events':self.eventsPerJob})
        return result


    def createJobSpec(self):
        """
        _createJobSpec_

        Load the WorkflowSpec object and generate a JobSpec from it

        """

        jobSpec = self.workflowSpec.createJobSpec()
        jobName = self.job_prefix + Aux.getSeparator(self.job_prefix)+'jobcut-' +\
            self.workflowSpec.workflowName()+'-'+str(self.count)
        self.currentJob = jobName
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count
        jobSpec.parameters['ProdMgr']='generated'



        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)
        jobSpec.payload.operate(self.generateCmsGenConfig)
        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)



        #  //
        # // Add site pref if set
        #//
        [ jobSpec.addWhitelistSite(x) for x in self.sites ]

        jobSpec.save(jobSpecFile)
        return jobSpecFile
