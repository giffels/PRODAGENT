#!/usr/bin/env python
"""
_EmulatorReportPlugin_

Plugin for the Job Emulator to generate job reports
upon job completion.  Successful reports are marked
as success while failure are marked a middleware
failures.

"""
__revision__ = "$Id: EmulatorReportPlugin.py,v 1.8 2008/04/03 21:04:41 fvlingen Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "sfoukes, sryu"

import logging
from random import randrange
from random import random
from random import gauss

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetDetails
from ProdCommon.MCPayloads.MergeTools import getSizeBasedMergeDatasetsFromNode
from ProdCommon.MCPayloads.UUID import makeUUID

from ProdAgent.WorkflowEntities import Job as WEJob
from ProdAgentCore.ProdAgentException import ProdAgentException

from JobEmulator.JobReportPlugins.JobReportPluginInterface \
     import JobReportPluginInterface
from JobEmulator.Registry import registerPlugin


class EmulatorReportPlugin(JobReportPluginInterface):
    """
    _EumlatorReportPlugin_
    
    Plugin for the Job Emulator to generate job reports
    upon job completion.  Successful reports are marked
    as success while failure are marked a middleware
    failures.

    """
    def createSuccessReport(self, jobSpecLoaded, jobRunningLocation):
        """
        _createSuccessReport_

        Create a job report representing the successful completion
        of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        jobSpecPayload, jobReportLocation, newReport = \
                self.__fwkJobReportCommon(jobSpecLoaded, jobRunningLocation)
        newReport.exitCode = 0
        newReport.status = "Success"

        if "jobId" in jobSpecLoaded.parameters.keys():
            newReport.jobSpecId = jobSpecLoaded.parameters["jobId"]
        
        # Create a list of datasets from the JobSpec
        # then associate file to these later on
        datasets = getOutputDatasetDetails(jobSpecPayload)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(jobSpecPayload))
        outModules = jobSpecPayload.cfgInterface.outputModules
        
        for dataset in datasets:
            modName = dataset.get('OutputModuleName', None)
            
            if outModules.has_key(modName):
                dataset['LFNBase'] = outModules[modName].get('LFNBase', None)
                self.setDefaultForNoneValue('LFNBase', dataset['LFNBase'])
                dataset['MergeedLFNBase'] = \
                                outModules[modName].get('MergedLFNBase', None)
        
        datasetMap = {}
        for dataset in datasets:
            datasetMap[dataset['OutputModuleName']] = dataset

        for outName, outMod in \
                jobSpecPayload.cfgInterface.outputModules.items():
            
            theFile = newReport.newFile()
            guid = makeUUID()
            theFile['LFN'] = "%s/%s.root" % (outMod['LFNBase'], guid)
            self.setDefaultForNoneValue('LFNBase', theFile['LFN'])
            theFile['PFN'] ="fakefile:%s" % theFile['LFN']
            theFile['GUID'] = guid
            theFile['ModuleLabel'] = outName
            theFile['Size'] = 500000 * randrange(5, 10)
            theFile.runs.append(jobSpecLoaded.parameters["RunNumber"])
            
            #check if the maxEvents['output'] is set if not set totalEvent using maxEvents['input']
            totalEvent = jobSpecPayload.cfgInterface.maxEvents['output']
            if totalEvent == None:
                totalEvent = jobSpecPayload.cfgInterface.maxEvents['input']
            
            # if there is no input and output, print out error message and set default to 1000
            totalEvent = self.setDefaultForNoneValue("maxEvent['input' and 'output']", totalEvent, 100)
            
            try:
                totalEvent = int(totalEvent)
            except ValueError, ex:
                logging.error("totalEvent is not a number. \n%s" % ex)
            
            if (random() > self.avgEventProcessingRate):
                # Gauss distribution of totalEvent.
                meanEvent = int(totalEvent * 0.7)
                stdDev = totalEvent * 0.15
                tempTotalEvent = int(gauss(meanEvent,stdDev))
                if tempTotalEvent <= 0 :
                    totalEvent = 1
                elif tempTotalEvent >= totalEvent:
                    totalEvent = totalEvent - 1
                else:
                    totalEvent = tempTotalEvent
            
            #logging.debug("---------- Total Event ----------: %s \n" % totalEvent)        
            theFile['TotalEvents'] = totalEvent
            
            theFile['SEName'] = jobRunningLocation['se-name'] 
            theFile['CEname'] = jobRunningLocation['ce-name']
            theFile['Catalog'] = outMod['catalog']
            theFile['OutputModuleClass'] = "PoolOutputModule"
            
            theFile.addChecksum("cksum", randrange(1000000, 10000000))
            theFile.branches.extend(["fakeBranch_%d-%s.Rec" % (num, guid) 
                                  for num in range(randrange(5,20))])
            #theFile.load(theFile.save())
            
            if datasetMap.has_key(outName):
                datasetForFile = theFile.newDataset()
                datasetForFile.update(datasetMap[outName])
            
                
        newReport.write(jobReportLocation)
             
        return
    
    def createFailureReport(self, jobSpecLoaded, jobRunningLocation):
        """
        _createFailureReport_

        Create a job report representing the failure of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        jobSpecPayload, jobReportLocation, newReport = \
                    self.__fwkJobReportCommon(jobSpecLoaded, jobRunningLocation)
        newReport.status = "Fail"
        newReport.exitCode = 1
        err = newReport.addError(1, "RandomEmulatorError")
        errDesc = "Failure in JobEmulator Layer \n"
        err['Description'] = errDesc
        newReport.workflowSpecId = jobSpecPayload.workflow
        
        newReport.write(jobReportLocation)
        
        return
    
    def __fwkJobReportCommon(self, jobSpecLoaded, jobRunningLocation):
        """
        __fwkJobReportCommon_

        Create a new job report and fill it in with generic
        information that is not dependent on the outcome of
        the job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.
        
        """
        #jobRunningLocation = RandomAllocationPlugin().allocateJob()
        
        try:
            jobSpecPayload = jobSpecLoaded.payload
            jobState = WEJob.get(jobSpecPayload.jobName)
            msg = "got state %s " % str(jobState)
            jobCache = jobState['cache_dir']
            msg += "test\n %s" % jobCache
            jobReportLocation = "%s/FrameworkJobReport.xml" % jobCache
            newReport = FwkJobReport()
            newReport.jobSpecId = jobSpecPayload.jobName
            newReport.jobType = jobSpecPayload.type
            newReport.workflowSpecId = jobSpecPayload.workflow
            newReport.name = jobSpecPayload.name
            #get information from the super class
            newReport.siteDetails['SiteName'] = jobRunningLocation['SiteName'] 
            #HostName is the same as worker_node name
            newReport.siteDetails['HostName'] = jobRunningLocation['HostName'] 
            newReport.siteDetails['se-name'] = jobRunningLocation['se-name'] 
            newReport.siteDetails['ce-name'] = jobRunningLocation['ce-name'] 
            return jobSpecPayload, jobReportLocation, newReport
                
        except Exception, ex:
            #msg = "Unable to Publish Report for %s\n" % jobSpecPayload.jobName
            #msg += "Since It is not known to the JobState System:\n"
            msg += str(ex)
            logging.error(msg)
            
            raise RuntimeError, msg
            
        
registerPlugin(EmulatorReportPlugin, EmulatorReportPlugin.__name__)

if __name__ == "__main__":
    from ProdCommon.MCPayloads.JobSpec import JobSpec
    jobSpec = JobSpec()
    jobSpec.load("/home/sryu")
