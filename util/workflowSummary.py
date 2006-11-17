#!/usr/bin/env python
"""
_workflowSummary_

Extract details from StatTracker DB and produce a summary for a given workflow

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
interval = "24:00:00"
doTiming = False


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




import StatTracker.StatTrackerAPI as Stats


if workflowName != None:
    print "Workflow:\t  %s " % workflowName
    print Stats.shortTextWorkflowSummary(workflowName)
    sys.exit(0)

allWorkflows = Stats.workflowSpecs() # all workflows known to StatTracker DB
for wf in allWorkflows:
    #  //
    # // filter based on command line match
    #//
    if wf.startswith(workflowMatch):
        print "Workflow:\t  %s " % wf
        print Stats.shortTextWorkflowSummary(wf)


        procSuccess = Stats.processingSuccessDetails(wf, interval)
        mergeSuccess = Stats.mergeSuccessDetails(wf, interval)

        procProps = {}
        mergeProps = {}
        
        for procJob in procSuccess:
            procProps[procJob['job_spec_id']] = procJob
            procProps[procJob['job_spec_id']]['attrs'] = \
               Stats.successfulJobProperties(procJob['job_index'])

        for mergeJob in mergeSuccess:
            mergeProps[mergeJob['job_spec_id']] = mergeJob
            mergeProps[mergeJob['job_spec_id']]['attrs'] = \
               Stats.successfulJobProperties(mergeJob['job_index'])
        
            
        if doTiming:
            procTimes = []
            mergeTimes = []
            
            for props in procProps.values():
                events = props['events_written']
                appStart = props['attrs']['timing']['AppStartTime']
                appEnd = props['attrs']['timing']['AppEndTime']
                timeTaken = int(appEnd) - int(appStart)
                timePerEvent = float(timeTaken) / float(events)
                procTimes.append(int(timePerEvent))


            avgProcTime = 0
            maxProcTime = 0
            minProcTime = 0
            if len(procTimes) > 0:
                avgProcTime = float(sum(procTimes)) / float (len(procTimes))
            maxProcTime = max(procTimes)
            minProcTime =  min(procTimes)

            msg = "Processing Times (secs):\tAvg Time/Event: "
            msg += "%s " % avgProcTime
            msg += "\tMin: %s  \tMax: %s \n" % (
                minProcTime, maxProcTime )
            

            
            print msg
            
            
            
        


#
#    print

sys.exit(0)



    
