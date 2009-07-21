#!/usr/bin/env python
"""
_T0LSFMonitor_

ResourceMonitor plugin for the T0 LSF submission system


"""

import logging

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgent.Resources.LSF import LSFInterface

from ProdAgent.WorkflowEntities import Job


class T0LSFMonitor(MonitorInterface):
    """
    _T0LSFMonitor_

    Monitor to watch the LSF Queues at CERN for T0 jobs

    Note that the RM supports multiple sites in the ResourceControlDB,
    In this case we are really only using one, which is CERN.

    """
    
    def __call__(self):

        #
        # check that at least on threshold is set
        #
        if (len(self.allSites) == 0):
            msg = "ERROR: No Resource Control Entry"
            raise RuntimeError, msg

        try:
            jobList = LSFInterface.bjobs()
        except Exception, ex:
            # can only happen if bjobs call failed
            # do nothing in this case, next loop will work
            logging.debug("Call to bjobs failed, just wait for next loop")
            return []

        #
        # two different ways to operate
        #
        # 1. threshold for defaultSiteName CERN is set
        #
        #   old-style method, only one set of
        #   processing and merge threshold 
        #
        # 2. threshold for defaultSiteName CERN is not set
        #
        #   system assumes every threshold is for
        #   a different workflow, with site == workflow
        #
        defaultSiteName = 'CERN'

        #
        # keep track of total number of jobs as list, first
        # element is processing jobs count, second element
        # is merge job count, third element is cleanup jobs 
        # forth element is repack job count
        # fifth is log collect, 6th is skim, 7th is express
        # 8th is DQM harvesting
        #
        jobCountOverall = [0,0,0,0,0,0,0,0]

        #
        # the jobCount dictionary contains workflows as keys and
        # the corresponding values are a list, with the first
        # element the processing job count, second the merge
        # job count and third the cleanup job 
        # forth repack job count
        #
        jobCountByWorkflow = {}
        if not self.allSites.has_key(defaultSiteName):
            for workflowId in self.allSites.iterkeys():
                jobCountByWorkflow[workflowId] = [0,0,0,0,0,0,0,0]

        #
        # loop over output of bjobs
        #
        for jobId in jobList.keys():
            if ( jobList[jobId] == 'PEND' or jobList[jobId] == 'RUN' ):

                # database query for job information from the workflow entities table
                # NOTE: might not work for ProdMgr jobs (not used for Tier0, so no problem)
                jobInfo = Job.get(jobId)

                if jobInfo != None:

                    jobType = jobInfo['job_type']
                    workflowId = jobInfo['workflow_id']

                    logging.debug("Found job %s from workflow %s" % (jobId,workflowId))

                    if ( jobType == 'Processing' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[0] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][0] += 1
                    elif ( jobType == 'Merge' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[1] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][1] += 1
                    elif ( jobType == 'CleanUp' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[2] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][2] += 1
                    elif ( jobType == 'Repack' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[3] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][3] += 1
                    elif ( jobType == 'LogCollect' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[4] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][4] += 1
                    elif ( jobType == 'Skim' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[5] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][5] += 1
                    elif ( jobType == 'Express' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[6] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][6] += 1
                    elif ( jobType == 'Harvesting' ):
                        if self.allSites.has_key(defaultSiteName):
                            jobCountOverall[7] += 1
                        elif jobCountByWorkflow.has_key(workflowId):
                            jobCountByWorkflow[workflowId][7] += 1

                else:
                    logging.debug("No job %s found in WE table" % jobId)

        if self.allSites.has_key(defaultSiteName):
            logging.info("Number of processing jobs is %d" % jobCountOverall[0])
            logging.info("Number of merge jobs is %d" % jobCountOverall[1])
            logging.info("Number of cleanup jobs is %d" % jobCountOverall[2])
            logging.info("Number of repack jobs is %d" % jobCountOverall[3])
            logging.info("Number of logCollect jobs is %d" % jobCountOverall[4])
            logging.info("Number of skim jobs is %d" % jobCountOverall[5])
            logging.info("Number of express jobs is %d" % jobCountOverall[6])
            logging.info("Number of harvesting jobs is %d" % jobCountOverall[7])
        else:
            for workflowId in self.allSites.iterkeys():
                logging.info("Number of processing jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][0]))
                logging.info("Number of merge jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][1]))
                logging.info("Number of cleanup jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][2]))
                logging.info("Number of repack jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][3]))
                logging.info("Number of logCollect jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][4]))
                logging.info("Number of skim jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][5]))
                logging.info("Number of express jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][6]))
                logging.info("Number of harvesting jobs for workflow %s is %d" % (workflowId,jobCountByWorkflow[workflowId][7]))

        result = []

        if self.allSites.has_key(defaultSiteName):
            self.releaseJobs(defaultSiteName,jobCountOverall,result,False)
        else:
            for workflowId in jobCountByWorkflow.iterkeys():
                self.releaseJobs(workflowId,jobCountByWorkflow[workflowId],result,True)

        return result


    def releaseJobs(self,siteName,jobCount,result,constrainWorkflow):

        siteData = self.allSites[siteName]
        siteIndex = siteData['SiteIndex']
        siteThresholds = self.siteThresholds[siteName]
        #siteAttrs = self.siteAttributes[siteName]

        processingThreshold = siteThresholds.get("processingThreshold", 1000000)
        mergeThreshold = siteThresholds.get("mergeThreshold", 1000000)
        cleanupThreshold = siteThresholds.get("cleanupThreshold", 1000000)
        repackThreshold = siteThresholds.get("repackThreshold", 10000000)
        collectThreshold = siteThresholds.get("logcollectThreshold", 1000000)
        skimThreshold = siteThresholds.get("skimThreshold", 1000000)
        expressThreshold = siteThresholds.get("expressThreshold", 1000000)
        harvestingThreshold = siteThresholds.get("harvestingThreshold", 1000000)
        
        missingProcessingJobs = processingThreshold - jobCount[0]
        missingMergeJobs = mergeThreshold - jobCount[1]
        missingCleanupJobs = cleanupThreshold - jobCount[2]
        missingRepackJobs = repackThreshold - jobCount[3]
        missingLogCollectJobs = collectThreshold - jobCount[4]
        missingSkimCollectJobs = skimThreshold - jobCount[5]
        missingExpressCollectJobs = expressThreshold - jobCount[6]
        missingHarvestingCollectJobs = harvestingThreshold - jobCount[7]

        minSubmit = siteThresholds.get("minimumSubmission", 1)
        maxSubmit = siteThresholds.get("maximumSubmission", 100)

        # check if we should release processing jobs
        if ( missingProcessingJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingProcessingJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingProcessingJobs

            # some more constraints
            constraint['type'] = "Processing"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Processing jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Processing jobs" % constraint['count'])

            result.append(constraint)

        # check if we should release merge jobs 
        if ( missingMergeJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingMergeJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingMergeJobs

            # some more constraints
            constraint['type'] = "Merge"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Merge jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Merge jobs" % constraint['count'])

            result.append(constraint)

        # check if we should release cleanup jobs 
        if ( missingCleanupJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingCleanupJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingCleanupJobs

            # some more constraints
            constraint['type'] = "CleanUp"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Cleanup jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Cleanup jobs" % constraint['count'])

            result.append(constraint)

         # check if we should release repack jobs 
        if ( missingRepackJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingRepackJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingRepackJobs

            # some more constraints
            constraint['type'] = "Repack"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # contraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d Repack jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d Repack jobs" % constraint['count'])

            result.append(constraint)


        # check if we should release logCollect jobs 
        if ( missingLogCollectJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingLogCollectJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingLogCollectJobs

            # some more constraints
            constraint['type'] = "LogCollect"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # constraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d logCollect jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d logCollect jobs" % constraint['count'])

            result.append(constraint)


        # check if we should release skim jobs 
        if ( missingSkimCollectJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingSkimCollectJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingSkimCollectJobs

            # some more constraints
            constraint['type'] = "Skim"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # constraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d skim jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d skim jobs" % constraint['count'])

            result.append(constraint)


        # check if we should release express jobs 
        if ( missingExpressCollectJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingExpressCollectJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingExpressCollectJobs

            # some more constraints
            constraint['type'] = "Express"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # constraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d express jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d express jobs" % constraint['count'])

            result.append(constraint)



        if ( missingHarvestingCollectJobs >= minSubmit ):

            constraint = self.newConstraint()

            # determine number of jobs
            if ( missingHarvestingCollectJobs > maxSubmit ):
                constraint['count'] = maxSubmit
            else:
                constraint['count'] = missingHarvestingCollectJobs

            # some more constraints
            constraint['type'] = "Harvesting"
            #constraint['site'] = self.allSites[siteName]['SiteIndex']

            # constraint for workflow
            if ( constrainWorkflow ):
                constraint['workflow'] = siteName
                logging.info("Releasing %d harvesting jobs for workflow %s" % (constraint['count'],siteName))
            else:
                logging.info("Releasing %d harvesting jobs" % constraint['count'])

            result.append(constraint)


        return

        
registerMonitor(T0LSFMonitor, T0LSFMonitor.__name__)
