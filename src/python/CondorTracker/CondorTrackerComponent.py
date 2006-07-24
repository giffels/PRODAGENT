#!/usr/bin/env python
"""
_CondorTrackingComponent_

Component that watches Condor and Job Caches to determine wether jobs have
completed sucessfully, or failed

"""
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from threading import Thread, Condition

from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from FwkJobRep.ReportState import checkSuccess

makePath = lambda x, y: os.path.join(x, y)
checkRep = lambda x: os.path.exists(os.path.join(x, "FrameworkJobReport.xml"))
beenChecked = lambda x: not os.path.exists(os.path.join(x, "CondorTracker"))

class CondorTrackerComponent:
    """
    _CondorTrackerComponent_

    ProdAgent component that polls looking for completed condor jobs

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args.setdefault("PollInterval", 10 )
        self.args.update(args)
        self.args['PollInterval'] = float(self.args['PollInterval'])
        #  //
        # // Default is to start polling resources right away
        #//
        self.activePolling = True

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("CondorTrackin Component Started...")
        self.cond = Condition()
        
        logging.getLogger().setLevel(logging.DEBUG)
        
    def __call__(self, event, payload):
        """
        _operator()_

        Define response to an Event and payload

        """
        logging.debug("Recieved Event: %s" % event)
        logging.debug("Payload: %s" % payload)
        
        if event == "CondorTracker:Start":
            logging.info("Starting Condor Tracker...")
            self.activePolling = True

        if event == "CondorTracker:Stop":
            logging.info("Stopping RM...")
            self.activePolling = False
            
            

        if event == "CondorTracker:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "CondorTracker:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        

                

    def startComponent(self):
        """
        _startComponent_

        Start component, subscribe to messages and start polling thread

        """
       
        # create message server
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("CondorTracker")
        self.ms.subscribeTo("CondorTracker:Start")
        self.ms.subscribeTo("CondorTracker:Stop")
        self.ms.subscribeTo("CondorTracker:StartDebug")
        self.ms.subscribeTo("CondorTracker:EndDebug")
        
        # start polling thread
        pollingThread = Poll(self.pollCondor)
        pollingThread.start()
        
        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CondorTracker: %s, %s" % (type, payload))
            self.__call__(type, payload)
            

    def listCacheDirs(self):
        """
        _listCacheDirs_

        Generate a list of all cache dirs in the JobCreator area

        """
        config = loadProdAgentConfiguration()
        compCfg = config.getConfig("JobCreator")
        if compCfg == None:
            logging.warning("JobCreator config not found")
            return []
        cacheDir = compCfg.get('ComponentDir', None)
        if cacheDir == None:
            logging.warning("No Cache Dir found for JobCreator")
            return []

        results = [ makePath(cacheDir, x) for x in os.listdir(cacheDir) \
                    if os.path.isdir(makePath(cacheDir, x))]
        results = filter(checkRep, results)
        results = filter(beenChecked, results)
        return results
        

    def isComplete(self, cacheDir):
        """
        _isComplete_

        Look at the Cache Dir and determine wether it is complete or not

        TODO: Based on file size of JobReport.
        
        """
        jobReport = os.path.join(cacheDir, "FrameworkJobReport.xml")
        if os.stat(jobReport)[6] == 0:
            return False
        logging.debug("Complete:%s" % cacheDir)
        return True

    def handleCompletion(self, cacheDir):
        """
        _handleCompletion_

        Check the completed job report, write the
        CondorTracker file into the cache
        """
        report = os.path.join(cacheDir, "FrameworkJobReport.xml")
        checked = os.path.join(cacheDir, "CondorTracker")
        file(checked, "w").close()
        
        if checkSuccess(report):
            logging.info("JobSuccess: %s" % report)
            self.ms.publish("JobSuccess", report)
            self.ms.commit()
            return
        else:
            logging.info("JobFailed: %s" % report)
            self.ms.publish("JobFailed", report)
            self.ms.commit()
            return
        

    def pollCondor(self):
        """
        _pollCondor_

        Find finished Jobs, publish success or failed events

        """
        returnValue = 0
        if not self.activePolling:
            logging.debug("pollCondor:Inactive")
        else:
            logging.debug("pollCondor:Active")
            self.cond.acquire()
            completeCache = []
            #  //
            # // Get list of completed jobs 
            #//
            completeCache = [i for i in self.listCacheDirs() \
                             if self.isComplete(i)]
            #  //
            # // For each completed job, publish the
            #//  appropriate event and write a file into the cache
            #  // to show it has been handled.
            # // 
            #//
            for item in completeCache:
                self.handleCompletion(item)
            self.cond.release()
        time.sleep(self.args['PollInterval'])
        return returnValue
    
    
        
class Poll(Thread):
    """
    Thread that performs polling
    """

    

    def __init__(self, poll):
        """
        __init__

        Initialize thread and set polling callback
        """
        Thread.__init__(self)
        self.poll = poll;

    def run(self):
        """
        __run__

        Performs polling 
        """
        while True:
            self.poll()
