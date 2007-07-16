#!/usr/bin/env python
"""
_SimpleMonitors_

Simple Monitor plugins that doesnt care about sites, it just
counts the number of jobs in the PA via either the BOSS DB or
JobStates DB and compares that to a threshold, and generates
a site-less resources available event


"""


from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


class PollInterface(dict):
    """
    _PollInterface_

    Basic interface definition for polling the number of jobs in the
    system.

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Total", 0)
        self.setdefault("Merge", None)
        self.setdefault("Processing", None)
        
    
    def __call__(self):
        """
        _operator()_

        Override this method to call out and count the number of jobs
        in the system, setting the dictionary fields based on
        job type.

        If not possible to split by type, set only Total field and
        leave Processing/Merge as None

        """
        raise NotImplementedError, "PollInterface.__call__()"


class PAJobStatePoll(PollInterface):
    """
    _PAJobStatePoll_

    Get job counts from JobStates DB

    """
    def __call__(self):
        """
        Query PA DB for jobs

        """
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()

        sqlStr1 = \
        """
        SELECT COUNT(JobSpecID) FROM js_JobSpec
           WHERE JobType="Processing" AND State='inProgress';
        """

        sqlStr2 = \
        """
        SELECT COUNT(JobSpecID) FROM js_JobSpec
           WHERE JobType="Merge" AND State='inProgress';
        """

        Session.execute(sqlStr1)
        numProcessing = Session.fetchone()[0]
        Session.execute(sqlStr2)
        numMerge = Session.fetchone()[0]
        Session.close_all()
        
        total = numProcessing + numMerge
        self['Total'] = total
        self['Processing'] = numProcessing
        self['Merge'] = numMerge
        return
    

class PABOSSPoll(PollInterface):
    """
    _PABOSSDBPoll_

    Poll the BOSSDB to get the total number of jobs, and counts based
    on job type if possible

    """
    def __call__(self):
        """
        _operator()_

        Query BOSS Here....

        """
        self['Total'] = 0
        self['Merge'] = None
        self['Processing'] = None
        return
        



class PAJobStateMonitor(MonitorInterface):
    """
    _PAJobStateMonitor_

    Basic Monitor plugin that uses a very simple threshold system
    for a "Default" site to release jobs without site preferences
    based on data from the PA JobState DB
    
    """
    def __call__(self):
        """
        _operator()_

        Get the default thresholds from the ResourceControlDB,
        poll the JobStates DB and calculate the difference

        """
        result = []

        #  //
        # // Get information for Default site
        #//
        siteName = "Default"
        if self.allSites.get(siteName, None) == None:
            #  //
            # // Cant do much if we can find a Default
            #//
            msg = "ERROR: No Resource Control Entry for site: %s" % siteName
            msg += "Need to have a site with this name defined..."
            raise RuntimeError, msg
        siteData = self.allSites[siteName]
        siteThresholds = self.siteThresholds[siteName]

        procThresh = siteThresholds.get("processingThreshold", None)
        if procThresh == None:
            msg = "No processingThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg
        mergeThresh = siteThresholds.get("mergeThreshold", None)
        if mergeThresh == None:
            msg = "No mergeThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg

        #  //
        # // Poll the JobStatesDB
        #//
        poller = PAJobStatesPoll()
        poller()

        #  //
        # // check the counts against the thresholds and make
        #//  resource constraints as needed
        if poller['Processing'] != None:
            test = poller['Processing'] - procThres
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Processing"
                result.append(constraint)
        if poller['Merge'] != None:
            test = poller['Merge'] - mergeThres
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                result.append(constraint)

        if (poller['Merge'] == None) and (poller['Processing'] == None):
            test = poller['Processing'] - procThres
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                result.append(constraint)

        #  //
        # // return the contstraints
        #//
        return result
    
            
        
    
    


class PABOSSMonitor(MonitorInterface):
    """
    _PABOSSMonitor_

    Basic Monitor plugin that uses a very simple threshold system
    for a "Default" site to release jobs without site preferences
    based on data from the PA BOSS DB
    
    """
    def __call__(self):
        """
        _operator()_

        Get the default thresholds from the ResourceControlDB,
        poll the BOSS DB and calculate the difference

        """
        pass
    
        

    

    

    
registerMonitor(PAJobStateMonitor, PAJobStateMonitor.__name__)
registerMonitor(PABOSSMonitor, PABOSSMonitor.__name__)