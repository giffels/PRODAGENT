 #!/usr/bin/env python
"""
_CondorG_

Tracker for CondorG submissions


"""

import logging
import popen2
import os
from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

from ResourceMonitor.Monitors.CondorQ import condorQ


def bulkCondorHistoryGet():
    """
    _bulkCondorHistoryGet_
    This gets a list of useful information from condor_history, parses it,
    returning a dictionary

    """

#  since condor_history basically sucks up the entire file anyway we'll do
#  that here first, then take what we need later

#  so far all we're going to be needing here is the status and the CondorId

    command="condor_history -constraint 'ProdAgent_JobID =!= UNDEFINED'"
    command += " -format \"%s \" ProdAgent_JobID  -format \"%d \" ClusterId -format \"%d \\n\" JobStatus"

#    command="condor_history"

    logging.debug("condor_history command:  %s \n" % command)
    pop = popen2.Popen4(command)
#   this bit of silliness is because condor_history will only print so
#   much before waiting for something to be read -- so if you just do
#   a wait() you then end up in a situation where you are waiting for
#   condor_history to finish waiting for you...  
    output = []
    histOutput = {}
    while (pop.poll() == -1):
        output += pop.fromchild.readlines()
    pop.wait()

#   OK, now we should have something...
    
    for line in output:
      pieces=line.split(" ")
#      if pieces[0] in paIds:
#         print "%s, %s, %s" % (pieces[0],pieces[1],pieces[2])
      try:
           histJobID=pieces[0]
           histOutput[histJobID]={}
           histOutput[histJobID]['ClusterId']=int(pieces[1])
           histOutput[histJobID]['JobStatus']=int(pieces[2])
           
      except ValueError, ex:
           logging.warning("Trouble parsing condor_history output...  Offending line:")
           logging.warning(line)
           
    return histOutput
    

    

class CondorG(TrackerPlugin):
    """
    _CondorG_

    Poll condor G for tracking information

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.classads = None
        self.cooloff = "00:2:00"

    def initialise(self):
        """
        _initialise_

        Retrieve data from condor Q

        """
        
        constraint = "\"ProdAgent_JobID =!= UNDEFINED\""

        self.classads = condorQ(constraint)
        logging.info("Retrieved %s Classads" % len(self.classads))

    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
        logging.info("CondorG: Submitted Count: %s" % len(submitted))
        histResults={}
        for subId in submitted:
            status = None
            classad = self.findClassAd(subId)
            if classad == None:
                if len(histResults)==0:                 
                  histResults=bulkCondorHistoryGet()
                if histResults.has_key(subId):
                  historyStatus=histResults[subId]['JobStatus']
                  logging.warning("Job %s doesn't have a classad...status=%s" % (subId,historyStatus))
                else:
                     # if there's no classad and there's nothing in the history, it evaporated
                     # scream and declare failure             
                     logging.warning("Job %s also wasn't in condor_history -- declaring Failure" % (subId))
                     self.TrackerDB.jobFailed(subId)
                     continue
            else:
                status = classad['JobStatus']

            logging.debug("Pre DB stuff: status:%s, subId: %s" % (status,subId))
            dbData = self.TrackerDB.getJob(subId, True)
            if not dbData['job_attrs'].has_key("CondorID"):
#              if we couldn't get a classad from condor_q status should still
#              be "None" at this point...  Use that to tell us to get stuff from history
                logging.debug("we should be putting something into the DB now")
                if status==None:
                   condorId=histResults[subId]['ClusterId']
                   
                else:    
                    condorId = classad['ClusterId']
                    
                logging.debug("inserting stuff into database: subID: %s, CondorID: %s" % (subId,condorId))
                self.TrackerDB.addJobAttributes(subId, CondorID = condorId)
            else:
                logging.debug("dbData had: %s" % dbData['job_attrs']["CondorID"][-1])
                condorId=dbData['job_attrs']["CondorID"][-1]

            if status==None:
                status=historyStatus

            if not status in range(1,6):
               logging.debug("Bad condor status flag: %s" % status)
               continue

            if status == 1:
                logging.debug("Job %s is pending" % (subId))
                continue 
            if status == 2:
                #  //
                # // Is running
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running" % (subId))
                continue
            if status == 4:
                #  //
                # // Is Complete -- but we want to forward to UpdateRunning first
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s complete" % (subId))
                continue
            if status == 5:
                #  //
                # // Held 
                #//
                logging.debug("Job %s is held..." % (subId))
                self.TrackerDB.jobFailed(subId)
#                self.TrackerDB.killJob(subId)
                command="condor_rm %s " % condorId
                logging.debug("Removing job from queue...")
                logging.debug("Executing %s " % command)
                os.system(command)
                continue
            if status in (3, 6):
                #  //
                # // Error or Removed
                #//
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has failed" % (subId))
                continue
            logging.debug("at end, status was %s" % status)
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("CondorG: Running Count: %s" % len(running))
        histResults={}
        historyStatus=None
        for runId in running:
            classad = self.findClassAd(runId)
            if classad == None:
                #  //
                # // No longer in queue => Finished
                #//  Check History to see how it finished
  
                dbData = self.TrackerDB.getJob(runId, True)
                clusterId = dbData['job_attrs']["CondorID"][-1]
                logging.debug("dbData had: %s" % dbData['job_attrs']["CondorID"][-1])
                if len(histResults)==0:                 
                  histResults=bulkCondorHistoryGet()
                if histResults.has_key(runId):
                  historyStatus=histResults[runId]['JobStatus']
                status=historyStatus
            else:     
                 status = classad['JobStatus']
                 clusterId = classad['ClusterId']

            if status in (3, 6):
                #  //
                # // Removed or Error
                #//
                self.TrackerDB.jobFailed(runId)
                if status == 3:
                     logging.debug("Job %s was removed, ClusterId=%s " % (runId,clusterId))
                else:
                     logging.debug("Job %s had an error, ClusterId=%s " % (runId,clusterId))
#                continue
            if status == 5:
                #  //
                # // Held
                #//
                logging.debug("Job %s is held..." % (runId))
                command="condor_rm %s " % clusterId
                logging.debug("Removing job from queue...")
                logging.debug("Executing %s " % command)
                os.system(command)
                self.TrackerDB.jobFailed(runId)
            if status == 2:
                logging.debug("Job %s is running, ClusterId=%s " % (runId,clusterId))
            if status == 4:
                logging.debug("Job %s is complete, ClusterId=%s" % (runId,clusterId))
                self.TrackerDB.jobComplete(runId)
            if status == 1:
                logging.debug("Job %s is idle? ClusterId=%s" % (runId,clusterId))
            if status > 6:
                logging.debug("Job %s status was %i, ClusterId=%s" % (runId,status,clusterId))
            
            
    def updateComplete(self, *complete):
        """
        _updateComplete_

        Take any required action on completion.

        Note: Do not publish these to the PA as success/failure, that
        is handled by the component itself

        """
        if len(complete) == 0:
            return
        summary = "Jobs Completed:\n"
        for compId in complete:
            summary += " -> %s\n" % compId
        logging.info(summary)
        return

    def updateFailed(self, *failed):
        """
        _updateFailed_

        Take any required action for failed jobs on completion

        """
        if len(failed) == 0:
            return
        summary = "Jobs Failed:\n"
        for compId in failed:
            
            
            summary += " -> %s\n" % compId
        logging.info(summary)
        return

    def kill(self, *toKill):
        """
        _kill_

        Lookup the cluster ID and do a condor_rm for each job spec ID provided

        """
        #TDB

    def cleanup(self):
        """
        _cleanup_

        """
        pass
        
        
    def findClassAd(self, jobspec):
        """
        _findClassAd_

        Look through list of classads and find entry with matching
        job spec id

        """
        for classad in self.classads:
            if classad['ProdAgent_JobID'] == jobspec:
                return classad
        return None

        
    

registerTracker(CondorG, CondorG.__name__)







