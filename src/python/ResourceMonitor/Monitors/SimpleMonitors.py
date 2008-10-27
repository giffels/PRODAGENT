#!/usr/bin/env python
"""
_SimpleMonitors_

Simple Monitor plugins that doesnt care about sites, it just
counts the number of jobs in the PA via either the BOSS DB or
JobStates DB and compares that to a threshold, and generates
a site-less resources available event


"""

import logging
from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

import commands
from ProdAgentCore.Configuration import loadProdAgentConfiguration

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
        self.setdefault("CleanUp", None)
        self.setdefault("LogCollect", None)
        self.setdefault("Repack", None)        
    
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
        SELECT COUNT(id) FROM we_Job
           WHERE job_type="Processing" AND status='inProgress';
        """
        sqlStr2 = \
        """
        SELECT COUNT(id) FROM we_Job
           WHERE job_type="Merge" AND status='inProgress';
        """
        sqlStr3 = \
        """
        SELECT COUNT(id) FROM we_Job
           WHERE job_type="CleanUp" AND status='inProgress';
        """

        sqlStr4 = \
        """
        SELECT COUNT(id) FROM we_Job
           WHERE job_type="LogCollect" AND status='inProgress';
        """

        sqlStr5 = \
        """
        SELECT COUNT(id) FROM we_Job
           WHERE job_type="Repack" AND status='inProgress';
        """        
        
        Session.execute(sqlStr1)
        numProcessing = Session.fetchone()[0]
        Session.execute(sqlStr2)
        numMerge = Session.fetchone()[0]
        Session.execute(sqlStr3)
        numClean = Session.fetchone()[0]
        Session.execute(sqlStr4)
        numLog = Session.fetchone()[0]
        Session.execute(sqlStr5)
        numRepack = Session.fetchone()[0]        
        Session.close_all()
        
        total = numProcessing + numMerge + numRepack
        self['Total'] = total
        self['Processing'] = numProcessing
        self['Merge'] = numMerge
        self['CleanUp'] = numClean
        self['LogCollect'] = numLog
        self['Repack'] = numRepack

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
        config = loadProdAgentConfiguration()
        BOSSconfig = config.getConfig("BOSS")
        bossCfgDir = BOSSconfig['configDir']


        sqlStr1 = \
        """
        select count(JOB.ID) from CHAIN,JOB where JOB.CHAIN_ID=CHAIN.ID and JOB.TASK_ID=CHAIN.TASK_ID and CHAIN.NAME not like '%merge%' and CHAIN.NAME not like '%CleanUp%' and JOB.STATUS not in ('W','SA','SD');
        """
        sqlStr2 = \
        """
        select count(JOB.ID) from CHAIN,JOB where JOB.CHAIN_ID=CHAIN.ID and JOB.TASK_ID=CHAIN.TASK_ID and CHAIN.NAME like '%merge%' and JOB.STATUS not in ('W','SA','SD');
        """
        sqlStr3 = \
        """
        select count(JOB.ID) from CHAIN,JOB where JOB.CHAIN_ID=CHAIN.ID and JOB.TASK_ID=CHAIN.TASK_ID and CHAIN.NAME like '%CleanUp%' and JOB.STATUS not in ('W','SA','SD');
        """
        sqlStr4 = \
        """
        select count(JOB.ID) from CHAIN,JOB where JOB.CHAIN_ID=CHAIN.ID and JOB.TASK_ID=CHAIN.TASK_ID and CHAIN.NAME like '%collect%' and JOB.STATUS not in ('W','SA','SD');
        """

        processingout=commands.getoutput("bossAdmin SQL -query \"%s\" -c %s"%(sqlStr1,bossCfgDir))
        numProcessing=long(processingout.strip().split('\n')[1])

        mergeout=commands.getoutput("bossAdmin SQL -query \"%s\" -c %s"%(sqlStr2,bossCfgDir))
        numMerge=long(mergeout.strip().split('\n')[1])

        cleanout=commands.getoutput("bossAdmin SQL -query \"%s\" -c %s"%(sqlStr3,bossCfgDir))
        numClean=long(cleanout.strip().split('\n')[1])
        
        cleanout=commands.getoutput("bossAdmin SQL -query \"%s\" -c %s"%(sqlStr4,bossCfgDir))
        numLog=long(cleanout.strip().split('\n')[1])


        #
        #BOSSdbConfig = dbConfig
        #BOSSdbConfig['dbName'] = "%s_BOSS"%(dbConfig['dbName'],)
        #
        #Session.set_database(BOSSdbConfig)
        #Session.connect()
        #Session.start_transaction()
        # 
        #Session.execute(sqlStr1)
        #numProcessing = Session.fetchone()[0]
        #Session.execute(sqlStr2)
        #numMerge = Session.fetchone()[0]
        #Session.close_all()
                                                                                                                           
        total = numProcessing + numMerge + numRepack
        self['Total'] = total
        self['Processing'] = numProcessing
        self['Merge'] = numMerge
        self['CleanUp'] = numClean
        self['LogCollect'] = numLog
        self['Repack'] = numRepack

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
            msg = "ERROR: No Resource Control Entry for site: %s \n" % siteName
            msg += "Need to have a site with this name defined..."
            raise RuntimeError, msg
        siteData = self.allSites[siteName]
        siteThresholds = self.siteThresholds[siteName]

        procThresh = siteThresholds.get("processingThreshold", None)
        if procThresh == None:
            msg = "No processingThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg
        repackThresh = siteThresholds.get("repackThreshold", None)
        if repackThresh == None:
            msg = "No repackThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg        
        mergeThresh = siteThresholds.get("mergeThreshold", None)
        if mergeThresh == None:
            msg = "No mergeThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg
        cleanThresh = siteThresholds.get("cleanupThreshold", None)
        if cleanThresh == None:
            msg = "No cleanupThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg
        logThresh = siteThresholds.get("logcollectThreshold", None)
        if logThresh == None:
            msg = "No logcollectThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg

        #  //
        # // Poll the JobStatesDB
        #//
        poller = PAJobStatePoll()
        logging.debug("Calling poller()")
        poller()
        logging.debug("Back from calling poller()")

        #  //
        # // check the counts against the thresholds and make
        #//  resource constraints as needed
        if poller['Processing'] != None:
            logging.info(" Processing jobs are: %s Threshold: %s"%(poller['Processing'],procThresh))
            test = poller['Processing'] - procThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Processing"
                result.append(constraint)
        if poller['Repack'] != None:
            logging.info(" Repack jobs are: %s Threshold: %s"%(poller['Repack'],repackThresh))
            test = poller['Repack'] - repackThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Repack"
                result.append(constraint)                
        if poller['Merge'] != None:
            logging.info(" Merge jobs are: %s Threshold: %s"%(poller['Merge'],mergeThresh))
            test = poller['Merge'] - mergeThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                result.append(constraint)
        
        if poller['CleanUp'] != None:
            logging.info(" CleanUp jobs are: %s Threshold: %s"%(poller['CleanUp'],cleanThresh))
            test = poller['CleanUp'] - cleanThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "CleanUp"
                result.append(constraint)

        if poller['LogCollect'] != None:
            logging.info(" LogCollect jobs are: %s Threshold: %s" % \
                         (poller['LogCollect'], logThresh) )
            test = poller['LogCollect'] - logThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "LogCollect"
                result.append(constraint)

        if (poller['Merge'] == None) and (poller['Processing'] == None) \
                    and (poller['CleanUp'] == None) \
                    and (poller['LogCollect'] == None):
            test = poller['Processing'] - procThresh
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
        result = []
                                                                                                                           
        #  //
        # // Get information for Default site
        #//
        siteName = "Default"
        if self.allSites.get(siteName, None) == None:
            #  //
            # // Cant do much if we can find a Default
            #//
            msg = "ERROR: No Resource Control Entry for site: %s \n" % siteName
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
        cleanThresh = siteThresholds.get("cleanupThreshold", None)
        if cleanThresh == None:
            msg = "No cleanupThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg
        logThresh = siteThresholds.get("logcollectThreshold", None)
        if logThresh == None:
            msg = "No logcollectThreshold found for site entry: %s\n" % (
                siteName,)
            raise RuntimeError, msg

        # Maximum number of processing jobs to submit in a single attempt for bulk ops.
        maxSubmit = siteThresholds.get("maximumSubmission", None)
                                                                                                                           
        #  //
        # // Poll the BOSSDB
        #//
        poller = PABOSSPoll()
        poller()
                                                                                                                           
        #  //
        # // check the counts against the thresholds and make
        #//  resource constraints as needed
        if poller['Processing'] != None:
            logging.info(" Processing jobs are: %s Threshold: %s "%(poller['Processing'],procThresh))
            test = poller['Processing'] - procThresh
            if test < 0:
                #// do not exceed the maximum number of bulk jobs : use bunch of maxSubmit jobs
                abstest=abs(test)
                if abstest > maxSubmit:
                    logging.info(" Max Submissions in bulk is: %s jobs. Do not allow bunch of jobs exceeding it:"%maxSubmit)
                    #// as many bunches as test/maxSubmit with maxSubmit jobs each
                    for i in range(1, int(abstest/maxSubmit)+1):
                        constraint = self.newConstraint()
                        constraint['count'] = abs(maxSubmit)
                        logging.info("  - bunch of %s jobs"%maxSubmit) 
                        constraint['type'] = "Processing"
                        result.append(constraint)
                    #// plus a bunch with the remaining mod jobs
                    jobsleft=int(abstest%maxSubmit)
                    if jobsleft:
                        constraint = self.newConstraint()
                        constraint['count'] = jobsleft
                        logging.info("  - bunch of %s jobs"%jobsleft)
                        constraint['type'] = "Processing"
                        result.append(constraint)
                else:   
                   constraint = self.newConstraint()
                   constraint['count'] = abs(test)
                   logging.info("  - bunch of %s jobs"%abs(test))
                   constraint['type'] = "Processing"
                   result.append(constraint)


        if poller['Merge'] != None:
            logging.info(" Merge jobs are: %s Threshold: %s"%(poller['Merge'],mergeThresh))
            test = poller['Merge'] - mergeThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                result.append(constraint)

        if poller['CleanUp'] != None:
            logging.info(" CleanUp jobs are: %s Threshold: %s"%(poller['CleanUp'],cleanThresh))
            test = poller['CleanUp'] - cleanThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "CleanUp"
                result.append(constraint)
                
        if poller['LogCollect'] != None:
            logging.info(" LogCollect jobs are: %s Threshold: %s" % \
                         (poller['LogCollect'], logThresh) )
            test = poller['LogCollect'] - logThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "LogCollect"
                result.append(constraint)

        if (poller['Merge'] == None) and (poller['Processing'] == None) and (poller['CleanUp'] == None):
            test = poller['Processing'] - procThresh
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                result.append(constraint)
                                                                                                                           
        #  //
        # // return the contstraints
        #//
        return result




    

    

    
registerMonitor(PAJobStateMonitor, PAJobStateMonitor.__name__)
registerMonitor(PABOSSMonitor, PABOSSMonitor.__name__)
