#!/usr/bin/env python
"""
_TrackerPlugin_

Interface class for Tracker Plugins

"""

import logging
import  CondorTracker.CondorTrackerDB as TrackerDB

class TrackerPlugin:
    """
    _TrackerPlugin_

    Interface object that gets instantiated and invoked to
    perform a set of update operations for watching and managing
    jobs

    """
    def __init__(self):
        self.TrackerDB = TrackerDB
        self.cooloff = "00:00:00"


    def __call__(self):
        """
        _operator()_

        Invoke the hooks in the plugin in the following order

        1. updateSubmitted - call with all submitted state job IDs
        2. updateRunning - call with all running state job IDs
        3. updateComplete - call with all complete state job IDs
        4. updateFailed -call with all failed state job IDs
        
        """

        self.initialise()

        #  //
        # // ToDo: Add exception wrappers for plugin calls
        #//
        subJobs = TrackerDB.getJobsByState("submitted", self.cooloff)
        self.updateSubmitted(*subJobs.keys())
        runningJobs = TrackerDB.getJobsByState("running")
        self.updateRunning(*runningJobs.keys())
        completeJobs = TrackerDB.getJobsByState("complete")
        self.updateComplete(*completeJobs.keys())
        failedJobs = TrackerDB.getJobsByState("failed")
        self.updateFailed(*failedJobs.keys())
        self.cleanup()

        return

    def initialise(self):
        """
        _initialise_

        Init phase, do everything you need to prep this object before the
        update methods.

        """
        logging.debug("TrackerPlugin.initialise")

    def cleanup(self):
        """
        _cleanup_

        After all methods, free up resources, cleanup any extra info
        etc before this instance gets whacked

        """
        logging.debug("TrackerPlugin.cleanup")



    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.debug("TrackerPlugin.updateSubmitted")
        for subId in submitted:
            logging.debug(" ==> %s" % subId)
        return

    
    def updateRunning(self, *running):
        """
        _updateRunning_

        Override to look at each running state job spec id provided
        and change its status if reqd.

        """
        logging.debug("TrackerPlugin.updateRunning")
        for runId in running:
            logging.debug(" ==> %s" % runId)
        return

    def updateComplete(self, *complete):
        """
        _updateComplete_

        Override to look at each complete state job spec id provided
        and change its status if reqd.

        """
        logging.debug("TrackerPlugin.updateComplete")
        for compId in complete:
            logging.debug(" ==> %s" % compId)
        return

    def updateFailed(self, *failed):
        """
        _updateFailed_

        Override to look at each failed state job spec id provided
        and change its status if reqd.

        """
        logging.debug("TrackerPlugin.updateFailed")
        for compId in failed:
            logging.debug(" ==> %s" % compId)
        return

    def kill(self, *soonToBeDead):
        """
        _kill_

        Take action to forcibly kill these jobs
        
        """
        logging.debug("TrackerPlugin.kill")
        for deadMeat in soonToBeDead:
            logging.debug(" ==> %s" % deadMeat)
        return
    
