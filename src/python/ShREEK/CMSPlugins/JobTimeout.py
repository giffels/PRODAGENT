#!/usr/bin/env python
"""
_JobTimeout_

If the total elapsed (real) time of the job exceeds a predefined
threshold provided in the monitor configuration, the job will be shutdown
at that time.

"""


from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.ApMon.DashboardAPI import DashboardAPI
import ShREEK.CMSPlugins.TraceUtils as TraceUtils

import os
import time
import signal
import popen2

class JobTimeout(ShREEKMonitor):
    """
    _JobTimeout_

    ShREEK Monitor that measures the time of the job against a cut off
    and shutsdown the job should it exceed that time limit
    
    """
    
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self.timeoutValue = None
        self.hardKillTimeoutDelay = None
        self.startTime = None
        self.checkTimeout = False
        self.triedSoftKill = False
        
    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_

        """
        if kwargs.has_key("Timeout"):
            # users think timeout is in minutes -- multiplying by 60
            self.timeoutValue = int(kwargs['Timeout'])*60
            # default hard kill is 2 hours after soft kill attempt
            self.hardKillTimeoutDelay = 7200
            self.startTime = int(time.time())
            self.checkTimeout = True

        if kwargs.has_key("HardKillDelay"):
            # users think this also is in minutes...
            self.hardKillTimeoutDelay = int(kwargs["HardKillDelay"])*60
            

          
        msg=""
        msg+="Job Timeout set: \n";
        msg += "Start Time: %s\n" % self.startTime
        msg += "Time Now: %s\n" % time.time()
        msg += "Timeout: %s\n" % self.timeoutValue
        msg += "Hard kill timeout delay: %s \n" % self.hardKillTimeoutDelay
        print msg



    def periodicUpdate(self, state):
        """
        _periodicUpdate_

        check time every periodic update cycle

        """
        if self.checkTimeout:
            timeDiff = int(time.time()) - self.startTime


            cmsRunProcess=-1
            whereIam=os.getcwd()
            fullPathtopid=os.path.join(whereIam,"process_id")
            if os.path.exists(fullPathtopid):
                filehandle=open(fullPathtopid,'r')
                output=filehandle.read()
                
                try:
                    cmsRunProcess=int(output)
                except ValueError:
                    pass

            #  //
            # // Note: If -1, no point carrying on here??
            #//
                
            if timeDiff > (self.timeoutValue) and  (not self.triedSoftKill):
                msg = "" 
                for lines in range(0,4):
                    for columns in range(0, 51):
                        msg += "#"
                    msg += "\n"
                msg += "WARNING: Soft Kill Timeout has Expired:"
                msg += "Start Time: %s\n" % self.startTime
                msg += "Time Now: %s\n" % time.time()
                msg += "Timeout: %s\n" % self.timeoutValue
                msg += "Gently Killing Job...\n"
                msg += "Process ID is: %s\n" % cmsRunProcess
                print msg
                self.triedSoftKill=True
                if cmsRunProcess != -1:
                    try:
                        os.kill(cmsRunProcess, signal.SIGUSR2)
                    except OSError:
                        pass
                    

            if timeDiff > self.timeoutValue+self.hardKillTimeoutDelay:
                msg = ""
                for lines in range(0,4):
                    for columns in range(0, 51):
                        msg += "#"
                    msg += "\n"
                msg += "WARNING: Hard Kill Timeout has Expired:"
                msg += "Start Time: %s\n" % self.startTime
                msg += "Time Now: %s\n" % time.time()
                msg += "Timeout: %s\n" % self.timeoutValue + self.hardKillTimeoutDelay
                msg += "Killing Job...\n"
                for lines in range(0,4):
                    for columns in range(0, 51):
                        msg += "#"
                    msg += "\n"

                if cmsRunProcess != -1:
                    self.tracebackProcess(cmsRunProcess)
                    
                
                print msg
                
                self.killJob()
        return
        

    def tracebackProcess(self, pid):
        """
        _tracebackProcess_

        Before killing, get strace and gdb output on where the thing
        is stuck
        """
        straceOut, straceErr = TraceUtils.strace(pid, 5)
        handle = open("./process-trace.log", 'w')
        handle.write("strace Output:\n")
        handle.write("\n%s\n" % straceOut)
        handle.write("strace Error:\n")
        handle.write("\n%s\n" % straceErr)
        handle.close()
        print "strace out:\n%s\n" % straceOut
        print "strace err:\n%s\n" % straceErr
        del straceOut, straceErr
        
        gdbOut, gdbErr = TraceUtils.gdbBacktrace(pid)
        handle = open("./process-trace.log", 'a')
        handle.write("GDB Backtrace Output:\n")
        handle.write("\n%s\n" % gdbOut)
        handle.write("GDB Bactrace Error:\n")
        handle.write("\n%s\n" % gdbErr)
        handle.close()
        print "gdb out:\n%s\n" % gdbOut
        print "gdb err:\n%s\n" % gdbErr
        del gdbOut, gdbErr
        
        return
        
        
                     
        
        
        
        
        
        
registerShREEKMonitor(JobTimeout, 'timeout')



