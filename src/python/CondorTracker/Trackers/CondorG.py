#!/usr/bin/env python
"""
_CondorG_

Tracker for CondorG submissions


"""

import logging
import popen2
from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker

from ResourceMonitor.Monitors.CondorQ import condorQ

#def condorHistoryStatus(clusterId):
# Changed this to key off the prodAgent ID which
# is actually saved under they key: ProdAgent_JobID

def condorHistoryStatus(paId):
    """
    _condorHistoryStatus_

    Get the exit status of the job from condor_history

    """
#  this was the old command    
#    command = "condor_history %s" % clusterId
#    command += " -format \"%d\n\" JobStatus"

    command = "condor_history -constraint 'ProdAgent_JobID == \""
    command += paId
    command += "\"' -format \"%d\\n\" JobStatus"
 
    pop = popen2.Popen4(command)
    pop.wait()
    status = pop.poll()
    if status:
        logging.debug("condor_history status... %s" %status)
        output = pop.fromchild.read().strip()
        logging.debug("output was: %s" % output)
        return None
    output = pop.fromchild.read().strip()
    try:
        result = int(output)
        logging.debug("condor_history returned: %s" % result)
        return result

 #  if the condor_history file becomes corrupted the call throws
 #  an exception, but still can return the right number.  For
 #  now we scream but forge ahead
    except ValueError, ex:
        lines=output.split("\n")
        lines.reverse()
        retval=int(lines.pop(0).strip())
        msg = "Exceptions were thrown in the condor_history call:\n"
        for line in lines:
            msg += line
        logging.warning(msg)
        if retval!=None:
          return retval
        else:
          msg="Retval was %s"%retval
          logging.debug(msg)
          return None
    


class CondorG(TrackerPlugin):
    """
    _CondorG_

    Poll condor G for tracking information

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.classads = None


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
        for subId in submitted:
            classad = self.findClassAd(subId)
            if classad == None:
                # wait for it to show up?? Cooloff period??
                # Set to complete?? Cant kill without Id...
                historyStatus = condorHistoryStatus(subId)
                # Catching jobs failed more or less spectacularly
                logging.warning("Job %s doesn't have a classad...status=%s" % (subId,historyStatus))
                # if there's no classad and there's nothing in the history, it evaporated
                logging.warning("Job %s also wasn't in condor_history -- declaring Failure" % (subId))
                self.TrackerDB.jobFailed(subId)
                continue


                
            status = classad['JobStatus']
            
            dbData = self.TrackerDB.getJob(subId, True)
            if not dbData['job_attrs'].has_key("CondorID"):
                condorId = classad['ClusterId']
                self.TrackerDB.addJobAttributes(subId, CondorID = condorId)
            if status == 1:
                logging.debug("Job %s is pending" % (subId))
                continue
            if status in (2, 4):
                #  //
                # // Is Runnning or Complete
                #//
                self.TrackerDB.jobRunning(subId)
                logging.debug("Job %s is running or complete" % (subId))
                continue
            if status == 5:
                #  //
                # // Held 
                #//
                self.TrackerDB.jobFailed(subId)
                self.TrackerDB.killJob(subId)
                logging.debug("Job %s is held, killed from DB" % (subId))
                
                continue
            if status in (3, 6):
                #  //
                # // Error or Removed
                #//
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has failed" % (subId))
                continue
            
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
        logging.info("CondorG: Running Count: %s" % len(running))
        for runId in running:
            classad = self.findClassAd(runId)
            if classad == None:
                #  //
                # // No longer in queue => Finished
                #//  Check History to see how it finished
                dbData = self.TrackerDB.getJob(runId, True)
                clusterId = dbData['job_attrs']["CondorID"][-1]
                historyStatus = condorHistoryStatus(runId)
                if historyStatus != 4:               
                    self.TrackerDB.jobFailed(runId)
                    logging.debug("Job %s has failed, status=%s, clusterId=%s" % (runId,historyStatus,clusterId))
                else:
                    self.TrackerDB.jobComplete(runId)
                    logging.debug("Job %s is complete" % (runId))
                continue
            #  //
            # // If still here, check status for held jobs etc and
            #//  kill/clean them out
            status = classad['JobStatus']
            if status in (3, 6):
                #  //
                # // Removed or Error
                #//
                self.TrackerDB.jobFailed(runId)
                if status == 3:
                     logging.debug("Job %s was removed" % (runId))
                else:
                     logging.debug("Job %s had an error" % (runId))
                continue
            if status == 5:
                #  //
                # // Held
                #//
                self.TrackerDB.jobFailed(runId)
                self.TrackerDB.killJob(runId)
                logging.debug("Job %s is held, killed from database" % (runId))
                continue
            if status == 2:
                logging.debug("Job %s is running" % (runId))
            if status == 4:
                logging.debug("Job %s is complete" % (runId))
            if status == 1:
                logging.debug("Job %s is idle?" % (runId))
            if status > 6:
                logging.debug("Job %s status was %i" % (runId,status))
            
            
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







