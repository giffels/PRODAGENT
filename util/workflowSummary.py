#!/usr/bin/env python
"""
_workflowSummary_

Extract details from prodMon and produce a summary for a given workflow

"""


import sys
import getopt

valid = ['workflows-matching=', 'interval=', 'workflow=', 'timing']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)
    sys.exit(1)

workflowMatch = None
workflowName = None
interval = 86400 #24 hours
doTiming = False
allWorkflows = []

for opt, arg in opts:
    if opt == "--workflows-matching":
        workflowMatch = arg
    if opt == "--interval":
        interval = arg
    if opt == "--workflow":
        workflowName = arg
    if opt == "--timing":
        doTiming = True

if workflowMatch == None:
    workflowMatch = ""


import ProdMon.ProdMonAPI as Stats



if workflowName != None:
    allWorkflows.append(workflowName)
else:
    allWorkflows = Stats.workflowSpecs() # all workflows known to ProdMon



for wf in allWorkflows:
    #  //
    # // filter based on command line match
    #//
    #\\ 
    # \\ but cut out CleanUp Jobs :)
    #  \\
    if not wf.startswith("CleanUp"):
      if wf.startswith(workflowMatch):
        print "Workflow:\t  %s " % wf
        print Stats.shortTextWorkflowSummary(wf)


        procSuccess = Stats.processingSuccessDetails(wf, interval)
        mergeSuccess = Stats.mergeSuccessDetails(wf, interval)
            
        if doTiming:
            procTimes = []
            mergeTimes = []

            for props in procSuccess:
                events = props['events_written']
                appStart = props['timing']['AppStartTime']
                appEnd = props['timing']['AppEndTime']
                timeTaken = int(appEnd) - int(appStart)
                timePerEvent = 0
                if events > 0:
                   timePerEvent = float(timeTaken) / float(events)
                procTimes.append(int(timePerEvent))


            avgProcTime = 0
            maxProcTime = 0
            minProcTime = 0
            if len(procTimes) > 0:
                avgProcTime = float(sum(procTimes)) / float (len(procTimes))
                maxProcTime = max(procTimes)
                minProcTime =  min(procTimes)


            for merps in mergeSuccess:
                events = merps['events_written']
                appStart = merps['timing']['AppStartTime']
                appEnd = merps['timing']['AppEndTime']
                timeTaken = int(appEnd) - int(appStart)
                timePerEvent = 0
                if events > 0:
                   timePerEvent = float(timeTaken) / float(events)
                mergeTimes.append(int(timeTaken))


            avgMergeTime = 0
            maxMergeTime = 0
            minMergeTime = 0
            if len(mergeTimes) > 0:
                avgMergeTime = float(sum(mergeTimes)) / float (len(mergeTimes))
                maxMergeTime = max(mergeTimes)
                minMergeTime =  min(mergeTimes)


            msg = "Processing Times (secs):\tAvg Time/Event: "
            msg += "%s " % avgProcTime
            msg += "\tMin: %s  \tMax: %s \n" % (minProcTime, maxProcTime )
            msg += "Merge Times (secs): \t\t Avg Time/Job: "
            msg += "%s " % avgMergeTime
            msg += "\tMin: %s  \tMax: %s \n" % (minMergeTime,maxMergeTime)
            

            
            print msg
            
            
            
        


#
#    print

sys.exit(0)



    
