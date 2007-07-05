#!/usr/bin/env python
"""
_dailyReport_

Create a processing report for all activity in the PA for the last 24 hours

"""

import time
import socket

import ProdMon.ProdMonDB as StatsDB
import ProdMon.ProdMonAPI as StatsAPI

from ProdAgentCore.Configuration import loadProdAgentConfiguration

interval = 86400 #24 hours
#dayInSeconds = 60*60*24
timenow = time.time()
timethen = timenow - interval

config = loadProdAgentConfiguration()
paName = config["ProdAgent"]["ProdAgentName"]
paHost = socket.gethostname()


header = " ProdAgent %s@%s Daily Report\n For %s to %s \n" % (
   paName, paHost, time.asctime(time.localtime(timethen)),
   time.asctime(time.localtime(timenow)),
   )

print header



workflows = StatsDB.activeWorkflowSpecs(interval)



def listToString(l):
    s = str(l)
    s = s.replace("[", "")
    s = s.replace("]", "")
    s = s.replace(",", "")
    s = s.replace("\'", "")
    return s

                


class Failures(dict):
    """
    _Failures_

    Formatter for failure summary

    """
    def __init__(self, jobType):
        dict.__init__(self)
        self.setdefault("Sites", [])
        self.setdefault("Type", jobType)
        self.setdefault("Errors", {})
        self.setdefault("ErrorTypes", {})
        self.setdefault("SiteErrors", {})
        self.setdefault("JobSpecList", [])
        
    def __call__(self, errDict):

        self['JobSpecList'].append(errDict['job_spec_id'])
        site = errDict["site_name"]
        if site not in self['Sites']:
            self['Sites'].append(site)
        errCode = errDict['exit_code']
        errType = errDict['error_type']
        if not self['Errors'].has_key(errCode):
            self['Errors'][errCode] = 0
        self['Errors'][errCode] += 1
        if not self['ErrorTypes'].has_key(errCode):
            self['ErrorTypes'][errCode] = errType
        
        if not self['SiteErrors'].has_key(errCode):
            self['SiteErrors'][errCode] = []
        self['SiteErrors'][errCode].append(site)
        return
    
        
    def __str__(self):
        result = "Failure Summary for %s Type jobs:\n" % self['Type']
        for err, count in self['Errors'].items():
            result += "Status: %s \tType: %s \tCount: %s \tSites: " % (
            err, self['ErrorTypes'][err], count )
            for site in self["Sites"]:
                numAtSite = self['SiteErrors'][err].count(site)
                if numAtSite != 0:
                    result += "%s @ %s " % (numAtSite, site)
            result += "\n"
            
        return result

        

      
class Successes(dict):
    """
    formatter tool for success summary

    """
    def __init__(self, jobtype):
        dict.__init__(self)
        self.setdefault("Type", jobtype)
        self.setdefault("Datasets", [])
        self.setdefault("SENames", [])
        self.setdefault("Timing", [])
        self.setdefault("StageOutTiming", [])

    def __call__(self, jobInfo):
        datasets = jobInfo['output_datasets']
        for ds in datasets:
            if ds not in self['Datasets']:
                self['Datasets'].append(ds)
        seName = jobInfo['se_name']
        if seName not in self['SENames']:
            self['SENames'].append(seName)

        events = jobInfo['events_written']
        appStart = jobInfo['timing']['AppStartTime']
        appEnd = jobInfo['timing']['AppEndTime']
        timeTaken = int(appEnd) - int(appStart)
        timePerEvent = float(timeTaken) / float(events)
        self['Timing'].append(int(timePerEvent))

        soStart = jobInfo['timing']['StageOutStart']
        soEnd = jobInfo['timing']['StageOutEnd']
        #print "%s - %s" % (soStart, soEnd)
        #soTime = int(soEnd) - int(soStart )
        soTime = 1
        self['StageOutTiming'].append(soTime)

    def __str__(self):
        result = "Success Summary for %s Type jobs:\n" % self['Type']

        avgProcTime = 0
        maxProcTime = 0
        minProcTime = 0
        if len(self['Timing']) > 0:
            avgProcTime = float(sum(self['Timing'])) / float (len(self['Timing']))
            maxProcTime = max(self['Timing'])
            minProcTime =  min(self['Timing'])
            result += "Timing: \tAvg Ev/sec %s \tMin: %s \tMax: %s\n" % (avgProcTime, minProcTime, maxProcTime)

        if len(self['StageOutTiming']) > 0:
            avgSO = float(sum(self['StageOutTiming']))/float(len(self['StageOutTiming']))
            maxSO = max(self['StageOutTiming'])
            minSO = min(self['StageOutTiming'])
            result += "StageOut: \tAvg PerJob %s " % avgSO
            result += "\tMin: %s \tMax: %s\n" % (minSO, maxSO)
            
        result += "Storage Elements: %s\n" % listToString(self['SENames'])
        for ds in self['Datasets']:
            result += "Output Dataset: %s\n" % ds
        
        return result


for wf in workflows:
    print "=========%s=========" % wf
    print StatsAPI.shortTextWorkflowSummary(wf, interval)
    
    procS = StatsAPI.processingSuccessDetails(wf, interval)
    mergeS = StatsAPI.mergeSuccessDetails(wf, interval)
    procF = StatsAPI.processingFailureDetails(wf, interval)
    mergeF = StatsAPI.mergeFailureDetails(wf, interval)

    
    if len(procS) > 0:
        procSSummary = Successes("Processing")
        for procSuccess in procS:
            procSSummary(procSuccess)
        print procSSummary

    if len(mergeS) > 0:
        mergeSSummary = Successes("Merge")
        for mergeSuccess in mergeS:
            mergeSSummary(mergeSuccess)
        print mergeSSummary
        
    if len(procF) > 0:
        procFSummary = Failures("Processing")
        for procFailure in procF:
            procFSummary(procFailure)
        print str(procFSummary)

    if len(mergeF) > 0:
        mergeFSummary = Failures("Merge")
        for mergeFailure in mergeF:
            mergeFSummary(mergeFailure)

        print str(mergeFSummary)
    
