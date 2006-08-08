#!/usr/bin/env python2.4

"""
_relValStats_

Generate a report from the StatTracker for a set of ReleaseValidation
jobs.


"""

import sys
import getopt

valid = ['workflows-matching=', 'interval=', 'make-lfn-lists']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print str(ex)
    sys.exit(1)

workflowMatch = None
interval = "24:00:00"
lfnLists = False

for opt, arg in opts:
    if opt == "--workflows-matching":
        workflowMatch = arg
    if opt == "--interval":
        interval = arg
    if opt == "--make-lfn-lists":
        lfnLists = True

if workflowMatch == None:
    workflowMatch = ""



import StatTracker.StatTrackerAPI as Stats

#  //
# // Util methods
#//
def sumOverList(x,y):
    return x+y

def averageOverList(l):
    return reduce(sumOverList, l) / len(l)

def summariseSuccesses(workflowSpec):
    """
    _summariseSuccesses_

    Convert successful jobs into a summary of successes

    """
    result = {
        "sites" : [],
        "eventsTotal" : 0,
        "seNames" : [],
        "lfns" : [],
        "avgTimePerEvent" : 0,
        "avgTimePerJob" : 0,
        }

    perJobTotalTimes = []
    perEventTimes = []
    successDetails = Stats.jobSuccessDetails(workflow, interval)
    for item in successDetails:
        siteName = item['site_name']
        seName = item['se_name']
        events = int(item['events_written'])
        if siteName not in result['sites']:
            result['sites'].append(siteName)
        if seName not in result['seNames']:
            result['seNames'].append(seName)
        
        result['eventsTotal'] += events
        props = Stats.successfulJobProperties(item['job_index'])
        result['lfns'].extend(props['output_files'])

        if props.has_key('timing'):
            startTime = props['timing']['AppStartTime']
            endTime = props['timing']['AppEndTime']
            
            totalTime = int(endTime) - int(startTime)
            perJobTotalTimes.append(totalTime)
            perEventTimes.append(totalTime / events)
    

    result['avgTimePerJob'] = averageOverList(perJobTotalTimes)
    result['avgTimePerEvent'] = averageOverList(perEventTimes)
    return result


def summariseFailures(workflowSpec):
    """
    _summariseFailures_

    Generate summary of failures

    """
    result = {
        "sites" : [],
        "exitCodes" : {},
        "hosts" : {},
        "failureCounts" : {},
        "failureDescriptions" : [],
        }
    
    failures = Stats.jobFailureDetails(workflowSpec, interval)
    for failure in failures:
        site = failure['site_name']
        host = failure['host_name']
        exitCode = failure['exit_code']
        errType  = failure['error_type']
        errDesc = failure['error_desc']
        if site not in result['sites']:
            result['sites'].append(site)
        if not result['hosts'].has_key(site):
            result['hosts'][site] = []
        result['hosts'][site].append(host)
        if not result['exitCodes'].has_key(exitCode):
            result['exitCodes'][exitCode] = errType
        if not result['failureCounts'].has_key(exitCode):
            result['failureCounts'][exitCode] = 0
        result['failureCounts'][exitCode] += 1
        result['failureDescriptions'].append((errType, errDesc))
    return result


def stringOverList(x, y):
    return str(x) +  ", %s" % y

def formatList(name, listInst):
    return "%s : %s\n" % (name, reduce(stringOverList, listInst))

def stringOverLFNList(x, y):
    return str(x) + " %s\n" % y



def printSummary(results):
    """
    _printSummary_

    Format output for stdout

    """
    msg = "=============%s==============\n" % results['Workflow']
    msg += " # Successes: %s\n" % results['SuccessfulJobs']
    msg += " # Failures: %s\n" % results['FailedJobs']
    msg += " Details:\n\n"
    successDetails = results['SuccessDetails']
    msg += " Total Events: %s\n" % successDetails['eventsTotal']
    msg += " Average Time Per Job: %s secs\n" % successDetails['avgTimePerJob']
    msg += " Average Time Per Event: %s secs\n" % successDetails['avgTimePerEvent']
    msg += formatList(" Sites", successDetails['sites'])
    msg += formatList(" SE Names", successDetails['seNames'])
    msg += " LFN List:\n "
    msg += reduce(stringOverLFNList, successDetails['lfns'])
    if results['FailedJobs'] == 0:
        print msg
        return
    
    failDetails = results['FailureDetails']
    msg += "\n Failure Details:\n"
    msg += formatList(" Failures@Sites", failDetails['sites'])
    msg += " Host details:\n"
    for key, value in failDetails['hosts'].items():
        msg += formatList( " Nodes@%s" % key, value)
    

    msg += " Exit Status:\n"
    msg += " Exit Code    |   count   |   error type   \n"
    msg += " ------------------------------------------\n"
    for key, value in failDetails['exitCodes'].items():
        msg += "   %s      |   %s       |  %s \n" % (
            key, failDetails['failureCounts'][key], value)


    msg += " Failure Descriptions:\n\n"
    for item in failDetails['failureDescriptions']:
        msg += " %s\n %s\n\n" % (item[0], item[1])
        
    print msg
    return

    

def makeLFNFile(results):
    """
    _makeLFNFile_

    Make txt file containing lfn list

    """
    fname = "%s.lfns" % results['Workflow']
    handle = open(fname, 'w')
    for lfn in results['SuccessDetails']['lfns']:
        handle.write("%s\n" % lfn)
    handle.close()
    return
    
    
#  //
# // Select the list of workflows to process
#//
allWorkflows = Stats.workflowSpecs() # all workflows known to StatTracker DB
workflows = []
for wf in allWorkflows:
    #  //
    # // filter based on command line match
    #//
    if wf.startswith(workflowMatch):
        workflows.append(wf)


for workflow in workflows:
    results = {"Workflow" : workflow }
    results['SuccessfulJobs']  = int(Stats.successfulJobCount(workflow))
    results['FailedJobs']  = int(Stats.failedJobCount(workflow))

    
    results['SuccessDetails'] = summariseSuccesses(workflow)

    results['FailureDetails'] = summariseFailures(workflow)
    
    printSummary(results)
    if lfnLists:
        makeLFNFile(results)


