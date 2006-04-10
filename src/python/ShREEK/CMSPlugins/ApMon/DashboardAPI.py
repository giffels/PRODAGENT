#!/usr/bin/python

"""
This is the Dashboard API Module for the Worker Node
"""

import ShREEK.CMSPlugins.ApMon.apmon as apmon
import time, sys, os
from types import DictType

# Method for writing debug information in a file
def logger(msg) :
    msg = `msg`
    if not msg.endswith('\n') :
        msg = msg + '\n'
    #print msg
    try :
        fh = open('report.log','a')
        fh.write(msg)
        fh.close
    except Exception, e :
        pass

##############
## CONTEXT

# Global context for report
#_context = {}

# Format envvar, context var name, context var default value
contextConf = {'MonitorID' : ('MonitorID', 'unknown'),
               'MonitorJobID'  : ('MonitorJobID', 'unknown'),
               'MonitorLookupURL': ('MonitorLookupURL', 'http://lxgate35.cern.ch:40808/ApMonConf') }
               
def getContext(overload={}) :
    if not isinstance(overload, DictType) :
        overload = {}
    context = {}
    for paramName in contextConf.keys() :
        paramValue = None
        if overload.has_key(paramName) :
            paramValue = overload[paramName]
        if paramValue is None :    
            envVar = contextConf[paramName][0] 
            paramValue = os.getenv(envVar)
        if paramValue is None :
            defaultValue = contextConf[paramName][1]
            paramValue = defaultValue
        context[paramName] = paramValue
    return context

## /CONTEXT
##############

# Methods for handling the apmon instance
# apmonConf = {'sys_monitoring' : False, 'job_monitoring' : False, 'general_info': False}

apm = None
counter = 0

def send(context, paramDict) :
    task = context['MonitorID']
    job = context['MonitorJobID']
    apmonUrl = context['MonitorLookupURL']
    global apm
    if apm is None : 
        logger("DboardReport: Creating ApMon with " + `apmonUrl`)
        apm = apmon.ApMon(apmonUrl, apmon.Logger.WARNING)
        logger("DboardReport: Destinations: " + `apm.destinations`) 
        #apm.enableBgMonitoring(False) 
        global counter
        counter += 1
        if counter > 1 :
            print "ERROR", "counter", counter
    logger("DboardReport: Sending("+task+","+job+","+`paramDict`+")")
    apm.sendParameters(task, job, paramDict)

# Reading the input arguments

_jobid = None
jobidEnvList = ['EDG_WL_JOBID', 'GLITE_WMS_JOBID']
def setGridJobID(argValues=None,default='unknown') :
    global _jobid
    if argValues is not None and argValues.has_key('GridJobID') :
        _jobid = argValues['GridJobID']
        argValues.__delitem__('GridJobID')
    if _jobid is None :
        for jobidEnvCandidate in jobidEnvList :
            jobidCandidate = os.getenv(jobidEnvCandidate)
            if jobidCandidate is not None :
                _jobid = jobidCandidate
                break
    if _jobid is None :
        _jobid = default
    
def getGridJobID() :
    if _jobid is None :
        setGridJobID()
    return _jobid
    
def getGridIdentity() :
    userid = os.popen("grid-proxy-info -identity").read().strip()
    return userid

# Simple filters (1-to-1 correspondance)
reportFilters = { 'getGridIdentity' : ('SyncGridName', getGridIdentity),
                  'SYNC' : ('SyncGridJobID', getGridJobID) }

# Complex filters (1-to-many relation)
reportCommands = {}

def readArgs(lines) :
    argValues = {}
    for line in lines :
        paramName = 'unknown'
        paramValue = 'unknown'
        line = line.strip()
        if line.find('=') != -1 :
            split = line.split('=')
            paramName = split[0]
            paramValue = '='.join(split[1:])
        else :
            paramName = line
        argValues[paramName] = paramValue
    return argValues    

def filterArgs(argValues) :

    contextValues = {}
    paramValues = {}
    command = None

    for paramName in argValues.keys() :

        if paramName in reportFilters.keys() :
            newParamName = reportFilters[paramName][0]
            newParamFilter = reportFilters[paramName][1]
            newParamValue = newParamFilter()
            argValues[newParamName] = newParamValue
            argValues.__delitem__(paramName)

        elif paramName in reportCommands.keys() :
            commandFilter = reportCommands[command]
            commandFilter(argValues)
            argValues.__delitem__(paramName)

    for paramName in argValues.keys() :
        paramValue = argValues[paramName]
        if paramValue is not None :
            if paramName in contextConf.keys() :
                contextValues[paramName] = paramValue
            else :
                paramValues[paramName] = paramValue 
        else :
            logger('Bad value for parameter :' + paramName) 
            
    return contextValues, paramValues

def report(args) :
    argValues = readArgs(args)
    setGridJobID(argValues)
    contextArgs, paramArgs = filterArgs(argValues)
    logger('context : ' + `contextArgs`) 
    logger('params : ' + `paramArgs`)
    context = getContext(contextArgs)
    send(context, paramArgs)
    time.sleep(1)
    #apm.free()
    print "Parameters sent to Dashboard."

class DashboardAPI :
    def __init__(self, monitorId = None, jobMonitorId = None, lookupUrl = None) :
        self.defaultContext = {}
        self.defaultContext['MonitorID']  = monitorId
        self.defaultContext['MonitorJobID']  = jobMonitorId
        self.defaultContext['MonitorLookupURL']  = lookupUrl
        pass

    def publish(self,**message) :
        contextArgs, paramArgs = filterArgs(message)
        for key in contextConf.keys() :
            if not contextArgs.has_key(key) and self.defaultContext[key] is not None :
                contextArgs[key] = self.defaultContext[key]
        context = getContext(contextArgs)
        send(context, paramArgs)
        
##
## MAIN PROGRAM
##
if __name__ == '__main__' :
    args = sys.argv[1:]
    if len(args) > 0 and args[0] == 'TEST' :
        apmSender = apmon.ApMon('http://lxgate35.cern.ch:40808/ApMonConf', apmon.Logger.WARNING)
        apmSender.sendParameters('Test', 'testjob_0_' + getGridJobID(), {'test':'0'})
        apmSender.free()
        sys.exit(0)
    report(args)
    sys.exit(0)
