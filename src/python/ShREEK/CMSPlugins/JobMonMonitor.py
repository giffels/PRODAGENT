#!/usr/bin/env python
"""
_JobMonMonitor_

JobMon ShREEK Monitor Plugin that runs a JobMon Daemon in a
thread and registers and unregisters the
job with a JobMon clarens server

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: JobMonMonitor.py,v 1.1 2006/03/16 18:38:20 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os
import threading
import socket

from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

from ShREEK.CMSPlugins.JobMon.JobMonDaemon import JobMonDaemon

class JobMonThread(threading.Thread):
    """
    _JobMonThread_

    Thread implementation to run the JobMon daemon as a thread
    
    """
    def __init__(self, **args):
        threading.Thread.__init__(self)
        self.args = {}
        jobname = "ShREEK-JobMonInterface-%s-%s" %(
            socket.gethostbyaddr(socket.gethostname())[0],
            os.getpid(),
            )
        self.args.setdefault("JobName", jobname)
        self.args.setdefault("CertFile", "/tmp/x509up_u%s" % os.getuid())
        self.args.setdefault("KeyFile", "/tmp/x509up_u%s" % os.getuid())
        self.args.setdefault("ServerURL", "https://fcdfcaf019.fnal.gov:8443/clarens")
        self.args.update(args)
        self.jobmonDaemon = None
        
    def run(self):
        """
        _run_
        
        Start the thread, instantiate the JobMonDaemon object that
        communicates with the JobMon Server
        
        """
        #  //
        # // Clarens client requires that HOME env var is set
        #//
        if not os.environ.has_key('HOME'):
            os.environ['HOME'] = os.getcwd()
            
        self.jobmonDaemon=JobMonDaemon(self.args["JobName"],
                                       self.args["CertFile"],
                                       self.args['KeyFile'],
                                       self.args['ServerURL'])
        self.jobmonDaemon.run()
        
        
        
    def jobEnd(self):
        """
        _jobEnd_
        
        unregister the job
        """
        self.jobmonDaemon.unregisterJob()
        



class JobMonMonitor(ShREEKMonitor):
    """
    _JobMonMonitor_

    ShREEKMonitor implementation for JobMon

    """
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self._JobMonClient = None
        
        
    def initMonitor(self, *posargs, **args):
        """
        _initMonitor_
        
        """
        if not args.has_key("JobName"):
            args['JobName'] = self.jobID

        if args.has_key("RequestName"):
            args['JobName'] = "%s-%s" % (args['RequestName'], args['JobName'])
            
        
        if args.has_key('CertFile'):
            args['CertFile'] = os.path.expandvars(args['CertFile'])
        if args.has_key('KeyFile'):
            args['KeyFile'] = os.path.expandvars(args['KeyFile'])
        self._JobMonClient = JobMonThread(**args)
        self._JobMonClient.setDaemon(1)
        self._JobMonClient.start()
        return

    def jobStart(self):
        """
        _jobStart_

        Attempt to register server with ApMonClient if it is initialised
        """
        print ">>>>>>>>JobMonMonitor.JobStart"
        #serverURL = self._JobMonClient.args["ServerURL"]
        return

    def periodicUpdate(self, state):
        state['JobMonContact'] = self._JobMonClient.args["ServerURL"]
        state['JobMonJobName'] = self._JobMonClient.args['JobName']
        return
    

    def shutdown(self):
        print ">>>>>>>>JobMonMonitor.shutdown"
        self._JobMonClient.jobEnd()
        del self._JobMonClient
        return

    
registerShREEKMonitor(JobMonMonitor, 'jobmon')
