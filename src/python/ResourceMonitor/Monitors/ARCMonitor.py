#!/usr/bin/env python
"""
_ARCMonitor_

ResourceMonitor plugin that monitors ARC resources

"""
import logging
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor
from ProdAgentCore.ResourceConstraint import ResourceConstraint
import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI
from ProdAgentCore.ResourceConstraint import ResourceConstraint
from ProdAgent.Resources import ARC

from popen2 import Popen4


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

        # Find out what existing processes of each type we have per CE
        procs = {}
        for ce in siteMap.keys():
            procs[ce] = {}
            for t in jobTypeMap.keys():
                procs[ce][t] = 0
        for p in ARC.getJobs():
            procs[p.CEName][p.jobType] = procs[p.CEName][p.jobType] + 1

        # Subtract the number of existing processes of each type per CE
        # from the allowed ones to get the constraints
        for ce in self.availableCEs():
            try:
                site = siteMap[ce]
            except:
                logging.info("Warning: CE %s not found in site list" % ce)
                continue
            thresh = ResConAPI.thresholdsByIndex(site)
            for type in jobTypeMap.keys():
                con = ResourceConstraint()
                con["count"] = thresh[jobTypeMap[type]] - procs[ce][type]
                con["type"] = type
                con["site"] = site
                logging.debug("%i counts for type %s at %s" % (con["count"], con["type"], con["site"]))

                constraints.append(con)
                totCount += con["count"]

        logging.info("ARCMonitor: %i constraints with a total count of %i\n" % \
                      (len(constraints), totCount))

        return constraints



    def availableCEs(self):
        """
        Return a list of CEs that are responding and who's queues are 'active'
        
        """
    
        sites = ResConAPI.activeSiteData()
        logging.debug("availableCEs: " + str(sites))

        if not sites: 
            logging.debug("ARC: No sites in ResConAPI.activeSiteData()")
            return []

        cmd = "ngstat -q"
        for s in sites:
            cmd += " -c " + s["CEName"]

        logging.debug("Executing command '%s'" % cmd)
        p = Popen4(cmd)
        x = p.wait()

        if x != 0: 
            logging.info("WARNING: ARC: Command '%s' failed" % cmd)
            return []

        asites = []
        for cluster in self.parseNgstatq(p):
            if cluster["Active"]: asites.append(cluster["CEName"])

        return asites
            


    def parseNgstatq(self, p):
        """
        Parse the output of 'ngstat -q' (read from p.fromchild) into a list of 
        {"CEName":str, "Alias":str, "Queue":str, "Active":bool}

        """

        r = []

        output = p.fromchild.readlines()
        i = 0
        while i < len(output):
            words = output[i].split()
            if words[0] == 'Cluster':
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
