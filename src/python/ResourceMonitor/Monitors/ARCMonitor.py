#!/usr/bin/env python
"""
_ARCMonitor_

ResourceMonitor plugin that monitors ARC resources

"""
import logging, os
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor
from ProdAgentCore.ResourceConstraint import ResourceConstraint
import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI
from ProdAgentCore.ResourceConstraint import ResourceConstraint
from ProdAgent.Resources import ARC

from popen2 import Popen4


# Mapping between job types and threshold types
jobTypeMap = {"Processing":"processingThreshold",
              "Merge":"mergeThreshold",
              "CleanUp":"cleanupThreshold" }


class ARCMonitor(MonitorInterface):
    """
    _CondorMonitor_

    Poll condor_q on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """
    def __call__(self):
        constraints = []
        totCount = 0
        siteMap = ResConAPI.createSiteIndexMap()

        # Find out how many existing jobs of each type we have per CE
        jobs = {}
        for ce in siteMap.keys():
            jobs[ce] = {}
            for jobType in jobTypeMap.keys():
                jobs[ce][jobType] = 0
        for j in ARC.getJobsLite():
            jobs[j.CEName][j.jobType] = jobs[j.CEName][j.jobType] + 1

        # Subtract the number of existing jobs of each type per CE
        # from the allowed ones to get the constraints
        for ce in self.availableCEs():
            try:
                site = siteMap[ce]
            except:
                logging.warning("CE %s not found in site list" % ce)
                continue
            threshold = ResConAPI.thresholdsByIndex(site)
            for (jobType, threshType) in jobTypeMap.items():
                con = ResourceConstraint()
                con["count"] = max(threshold[threshType] - jobs[ce][jobType], 0)
                con["type"] = jobType
                con["site"] = site
                logging.debug("%i counts for type %s at %s" % (con["count"], con["type"], con["site"]))

                constraints.append(con)
                totCount += con["count"]

        logging.info("ARCMonitor: %i constraints with a total count of %i\n" % \
                      (len(constraints), totCount))

        return constraints


    def availableCEs(self):
        """ 
        Return a list of CEs that are responding and who's queues are
        'active', and who's corresponding SEs appears to be up and running.
        
        """
    
        sites = ResConAPI.activeSiteData() 
        logging.debug("availableCEs: " + str(sites))

        if not sites: 
            logging.debug("ARC: No sites in ResConAPI.activeSiteData()")
            return []

        cmd = "ngstat -q"
        for s in sites:
            cmd += " -c " + s["CEName"]

        try:
            output = ARC.executeCommand(cmd)
        except ARC.CommandExecutionError, msg:
            logging.warning("Didn't get information on ARC resources: " + msg)
            return []

        seMap = ResConAPI.createSEMap()
        asites = []
        for cluster in self.parseNgstatq(output.split('\n')):
            CEName = cluster["CEName"]
            if cluster["Active"] and self.SEWorks(seMap[CEName]):
                asites.append(CEName)

        return asites
            

    def SEWorks(self, SEName):
        """
        Return True if the SE is up and running.
        """

        # Check availability by srmpinging the server. 
        # FIXME: Is this the best way to do it? What about XRoot?
        r = os.system("srmping srm://" + SEName) == 0
        logging.debug("SE " + SEName + " is up: " + str(r))
        return r



    def parseNgstatq(self, output):
        """
        Parse the output (list of lines) of 'ngstat -q' into a list of 
        {"CEName":str, "Alias":str, "Queue":str, "Active":bool}

        """

        r = []
        i = 0
        while i < len(output):
            words = output[i].split()

            if len(words) > 0 and words[0] == 'Cluster':
                CEName = words[1]

                alias, queue, active = "", "", False
                while i < len(output) and output[i].strip():
                    words = output[i].split()
                    if words[0] == 'Alias:': 
                        alias = words[1]
                    elif words[0] == 'Queue':
                        queue = words[1]
                    elif words[0] == 'Status:' and words[1] == 'active':
                        active = True
                    i += 1

                r.append({"CEName":CEName, "Alias":alias, "Queue":queue, "Active":active})
            i += 1

        return r


    
registerMonitor(ARCMonitor, ARCMonitor.__name__)
