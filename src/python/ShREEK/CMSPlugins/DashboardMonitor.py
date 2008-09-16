#!/usr/bin/env python
"""
_DashboardMonitor_

MonALISA ApMon based monitoring plugin for ShREEK to broadcast data to the
CMS Dashboard

"""
__version__ = "$Revision: 1.14 $"
__revision__ = "$Id: DashboardMonitor.py,v 1.14 2008/05/06 11:12:53 swakef Exp $"
__author__ = "evansde@fnal.gov"



from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo, extractDashboardID
from ShREEK.CMSPlugins.ApMonLite.ApMonDestMgr import ApMonDestMgr
from ShREEK.CMSPlugins.EventLogger import EventLogger
from IMProv.IMProvQuery import IMProvQuery

from ProdCommon.FwkJobRep.ReportParser import readJobReport

import os
import time
import socket
import popen2

_GridJobIDPriority = [
    'EDG_WL_JOBID',
    'GLITE_WMS_JOBID',
    'GLOBUS_GRAM_JOB_CONTACT',
    ]


def getSyncCE():
    """
    _getSyncCE_

    Extract the SyncCE from GLOBUS_GRAM_JOB_CONTACT if available for OSG,
    otherwise broker info for LCG

    """
    result = socket.gethostname()

    if os.environ.has_key('GLOBUS_GRAM_JOB_CONTACT'):
        #  //
        # // OSG, Sync CE from Globus ID
        #//
        val = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        try:
            host = val.split("https://", 1)[1]
            host = host.split(":")[0]
            result = host
        except:
            pass
        return result
    if os.environ.has_key('EDG_WL_JOBID'):
        #  //
        # // LCG, Sync CE from edg command
        #//
        command = "glite-brokerinfo getCE"
        pop = popen2.Popen3(command)
        pop.wait()
        exitCode = pop.poll()
        if exitCode:
            return result 
        
        content = pop.fromchild.read()
        result = content.strip()
        return result
    return result

class DashboardMonitor(ShREEKMonitor):
    """
    _DashboardMonitor_

    ShREEK Monitor that broadcasts data to the CMS Dashboard using ApMon

    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.destPort = None
        self.destHost = None
        self.dashboardInfo = None
        self.lastExitCode = None
        self.eventFile = "EventLogger.log"
        self.eventLogger = None
        self.currentTask = None
        self.apmon = None
        self.syncCE = getSyncCE()

    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        self.destHost = kwargs['ServerHost']
        self.destPort = int(kwargs['ServerPort'])
        dashboardInfoFile = kwargs['DashboardInfo']
        dashboardInfoFile = os.path.expandvars(dashboardInfoFile)

        jobSpecFile = os.environ.get("PRODAGENT_JOBSPEC", None)
        if jobSpecFile == None:
            msg = "No JobSpec file found in job...\n"
            msg += "Aborting dashboard publication...\n"
            print msg
            return

        
        self.dashboardInfo = DashboardInfo()
        try:
            self.dashboardInfo.read(dashboardInfoFile)
        except Exception, ex:
            msg = "Unable to read DashboardInfo file\n"
            msg += "%s\n" % str(ex)
            msg += "Some information may be missing..."
            print msg
            
        self.dashboardInfo.task, self.dashboardInfo.job = \
                                 extractDashboardID(jobSpecFile)
        
        try:
            self.dashboardInfo.addDestination(self.destHost, self.destPort)
        except Exception, ex:
            msg = "Error adding Dashboard Destination\n"
            msg += "Unable to communicate to Dashboard\n"
            print msg
            self.dashboardInfo = None
            return


        cluster = self.dashboardInfo.task
        node = kwargs.get('ProdAgentJobID', None)
        if node == None:
            return
        self.apmon = ApMonDestMgr(cluster, node)
        destQ = IMProvQuery("ShREEKMonitorCfg/EventDestination")
        dests = destQ(self.monitorConfig)
        for dest in dests:
            destPort = int(dest.attrs['Port'])
            destHost = str(dest.attrs['Host'])
            self.apmon.newDestination(destHost, destPort)
            
    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """  
        del self.dashboardInfo
        
    def jobStart(self):
        """
        Job start notifier.
        """
        if self.dashboardInfo == None:
            return

       
        gridJobId = None
        for envVar in _GridJobIDPriority:
            val = os.environ.get(envVar, None)
            if val != None:
                gridJobId = val
                break
        print "Dashboard Grid Job ID: %s" % gridJobId
        if gridJobId != None:
            self.dashboardInfo.job = "%s_%s" % (self.dashboardInfo.job, gridJobId)
        self.dashboardInfo['GridJobID'] = gridJobId
        self.dashboardInfo['JobStarted'] = time.time()
        self.dashboardInfo['SyncCE'] = getSyncCE()
        self.dashboardInfo.publish(1)
        return

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        if self.dashboardInfo == None:
            return
        self.currentTask = task
        newInfo = self.dashboardInfo.emptyClone()
        newInfo.addDestination(self.destHost, self.destPort)
        newInfo['ExeStart'] = task.taskname()
        newInfo['ExeStartTime'] = time.time()
        newInfo.publish(1)
        return
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        if self.dashboardInfo == None:
            return

        exitFile = os.path.join(task.directory(), "exit.status")
        exitValue = exitCode
        if os.path.exists(exitFile):
            content = file(exitFile).read().strip()
            try:
                exitValue = int(content)
            except ValueError:
                exitValue = exitCode
                
                
        self.lastExitCode = exitValue
        newInfo = self.dashboardInfo.emptyClone()
        newInfo.addDestination(self.destHost, self.destPort)
        newInfo['ExeEnd'] = task.taskname()
        newInfo['ExeFinishTime'] = time.time()
        newInfo['ExeExitStatus'] = exitValue
        newInfo.publish(1)



        self._LoadEventLogger()
        self._CheckEvents()
        lastRun = 0
        lastEvent = 0
        if self.eventLogger != None:
            lastRun = self.eventLogger.latestRun
            lastEvent = self.eventLogger.latestEvent
            
        self.eventLogger = None
        if self.apmon == None:
            return
        self.apmon.connect()
        for i in range(0, 1):
            self.apmon.send(
                 SyncCE = self.syncCE,
                 NEvents = lastEvent)            
        self.apmon.disconnect()
        
        return

    def jobEnd(self):
        """
        Job ended notifier.
        """
        if self.dashboardInfo == None:
            return
        newInfo = self.dashboardInfo.emptyClone()
        newInfo.addDestination(self.destHost, self.destPort)
        try:
            reports = readJobReport("FrameworkJobReport.xml")
            newInfo['JobExitStatus'] = reports[-1].exitCode
        except:
            newInfo['JobExitStatus'] = 50116
        newInfo['JobFinished'] = time.time()
        newInfo.publish(1)

    def periodicUpdate(self, monitorState):
        """
        Periodic update.
        """
        self._LoadEventLogger()
        self._CheckEvents()


        if self.eventLogger == None:
            return

        if self.apmon == None:
            return
        self.apmon.connect()
        self.apmon.send(
            SyncCE = self.syncCE,
            NEvents = self.eventLogger.latestEvent)
        #RunNumber = self.eventLogger.latestRun,
        self.apmon.disconnect()
        return
    
    def _LoadEventLogger(self):
        """
        _LoadEventLogger_

        Look for file and load it if it exists

        """
        if self.eventLogger != None:
            return
        if not os.path.exists(self.eventFile):
            self.eventLogger = None
        else:
            self.eventLogger = EventLogger(self.eventFile)
        return

    def _CheckEvents(self):
        """
        _CheckEvents_

        Get last available run/event number

        """
        if self.eventLogger == None:
            return

        try:
            self.eventLogger()
        except Exception, ex:
            print "Error Calling Event Logger:", ex
            pass
        return 


        
registerShREEKMonitor(DashboardMonitor, 'dashboard')
