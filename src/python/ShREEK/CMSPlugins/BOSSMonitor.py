#!/usr/bin/env python
"""
_BOSSMonitor_

ShREEKMonitor implementation that generates messages for BOSS on stdout


"""

from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

import os
import time
import string,popen2


class BOSSMonitor(ShREEKMonitor):
    """
    _BOSSMonitor_

    Echo useful information to stdout for BOSS filters


    """
    _Prefix = "<BOSS>"

    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        


    def initMonitor(self, *args, **kwargs):
        """
        Init method that can accept positional and keyword args
        this should be used to init the object
        """
        print self._Prefix, "BOSSMonitor.initMonitor called"

    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """
        print self._Prefix, "BOSSMonitor.shutdown called"


    #  //=====================================================
    # //   Monitoring Hook methods, override as needed.
    #//
    def periodicUpdate(self, monitorState):
        """
        Periodic update.
        """
        for key, value in monitorState.items():
            print self._Prefix, "%s=%s" % (key, value)


    #  //
    # // Start of job notifier
    #//
    def jobStart(self):
        """
        Job start notifier.
        """
        print self._Prefix, "BOSSMonitor.jobStart Started=%s"%time.time()
 
        print self._Prefix, "MemTotal: %s"%self.getMemTotal()
        cpu,ncpu=self.getCPU()
        print self._Prefix, "cpu MHz: %s"%cpu

    #  //
    # // Task started
    #//
    def taskStart(self, task):
        """
        Tasked started notifier. 
        """
        print self._Prefix, "BOSSMonitor.taskstart Task=%s Started=%s" %(task.taskname(),time.time())
    
    def taskEnd(self, task, exitCode):
        """
        Tasked ended notifier.
        """
        print self._Prefix, "BOSSMonitor.taskEnd Task=%s Exit=%s Ended=%s" % (
            task.taskname(), exitCode, time.time()
            )

        
    def jobEnd(self):
        """
        Job ended notifier.
        """
        print self._Prefix, "BOSSMonitor.jobEnd Ended=%s"%time.time()
        print self._Prefix, "Dump Final FrameworkJobReport "
        jobReport="FrameworkJobReport.xml"
        if os.path.exists(jobReport):
            handle = open(jobReport, 'r')
            print handle.read()
        else:
            print "NOT FOUND: %s" % jobReport

        print self._Prefix, "End Dump Final FrameworkJobReport "

    def jobKilled(self):
        """
        Job killed notifier.
        """
        print self._Prefix, "BOSSMonitor.jobKilled"


    def getMemTotal(self):
        """
        get memory 
        """

        pop = popen2.Popen4("less /proc/meminfo | grep \"MemTotal\"")
        while pop.poll() == -1:
         exitCode = pop.poll()
        exitCode = pop.poll()
                                                                                                                 
        output = pop.fromchild.read().strip()
        mem='unknown'
        if output.count(':') >0:
           mem=output.split(':')[1]
                                                                                                                 
        return mem.strip()

    def getCPU(self):
        """
        get CPU
        """

        pop = popen2.Popen4("less /proc/cpuinfo | grep \"cpu MHz\"")
        while pop.poll() == -1:
         exitCode = pop.poll()
        exitCode = pop.poll()
                                                                                                                 
        output = pop.fromchild.read().strip()
        cpuList=output.split('\n')
        cpunumber=len(cpuList)
        cpu='unknown'
        if output.count(':') >0:
          cpu=cpuList[0].split(':')[1].strip()
                                                                                                                 
        return cpu,cpunumber




registerShREEKMonitor(BOSSMonitor, 'boss')
