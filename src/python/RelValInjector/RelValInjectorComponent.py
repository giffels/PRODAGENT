#!/usr/bin/env python
"""
_RelValInjectorComponent_

Component that injects and manages RelVal jobs when provided with
a RelVal spec file, tracks the jobs in its own database and
triggers merges and export when jobs complete

"""
import os
import time
import logging

from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils

from RelValInjector.RelValSpecMgr import RelValSpecMgr


from JobQueue.JobQueueAPI import bulkQueueJobs


class RelValInjectorComponent:
    """
    _RelValInjectorComponent_

    Component to inject, trace and manage RelVal jobs

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['CurrentArch'] = None
        self.args['CurrentCMSPath'] = None
        self.args['CurrentVersion'] = None
        self.args['FastJob'] = 250
        self.args['MediumJob'] = 100
        self.args['SlowJob'] = 50
        self.args['SitesList'] = None
        self.args['PollInterval'] = "00:10:00"
        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        #  //
        # // Job class number of events should be ints
        #//
        self.args['FastJob'] = int(self.args['FastJob'])
        self.args['MediumJob'] = int(self.args['MediumJob'])
        self.args['SlowJob'] = int(self.args['SlowJob'])
        self.args['Fast'] = self.args['FastJob']
        self.args['Medium'] = self.args['MediumJob']
        self.args['Slow'] = self.args['SlowJob']
        
        #  //
        # // List of sites to get RelVal jobs
        #//
        self.sites = []
        for sitename in self.args['SitesList'].split(','):
            if len(sitename.strip()) > 0:
                self.sites.append(sitename.strip())
        

        LoggingUtils.installLogHandler(self)
        msg = "RelValInjector Component Started:\n"
        msg += " Current Release: %s\n" % self.args['CurrentVersion']
        msg += " Current Arch: %s\n" % self.args['CurrentArch']
        msg += " Current CMS_PATH: %s\n" % self.args['CurrentCMSPath']
        msg += "Jobs to be sent to Sites:\n"
        for site in self.sites:
            msg += " ==> %s\n" % site
            
        
        logging.info(msg)
        
        


    def __call__(self, message, payload):
        """
        _operator()_

        Respond to messages

        """
        logging.debug("Message Recieved: %s %s" % (message, payload))

        if message == "RelValInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if message == "AdminControl:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        if message == "RelValInjector:SetCurrentVersion":
            self.args['CurrentVersion'] = payload
            return
        if message == "RelValInjector:SetCurrentArch":
            self.args['CurrentArch'] = payload
            return
        if message == "RelValInjector:SetCurrentCMSPath":
            self.args['CurrentCMSPath'] = payload
            return

        if message == "RelValInjector:Inject":
            self.inject(payload)
            return

        if message == "RelValInjector:Poll":
            self.poll()
            return

    def poll(self):
        """
        _poll_

        Polling loop response to check status of RelVal jobs being tracked
        
        """
        logging.info("RelValInjector.poll()")
        #  //
        # // TODO: Poll DB tables for complete workflows  
        #//
        #self.ms.publish("RelValInjector:Poll", "",
        #                self.args['PollInterval'])
        #self.ms.commit()        
        

    def inject(self, relValSpecFile):
        """
        _inject_

        Given the relVal spec file provided, take that and generate
        workflows and jobs for all sites

        """
        if not os.path.exists(relValSpecFile):
            msg = "Cannot load RelVal Spec File:\n  %s\n" % relValSpecFile
            msg += "File does not exist..."
            logging.error(msg)
            return

        #  //
        # // Check variables pointing to the release are all set
        #//
        for argName in ['CurrentVersion', 'CurrentArch', 'CurrentCMSPath']:
            if self.args[argName] == None:
                msg = "Unable to create workflows:\n"
                msg += "Variable  %s not set!\n" % argName
                msg += "This variable should be set either in the PA Config\n"
                msg += "Or via a RelValInjector:Set%s event" % argName
                logging.error(msg)
                return
        
        
        specMgr = RelValSpecMgr(relValSpecFile, self.sites, **self.args)

        try:
            tests = specMgr()
        except Exception, ex:
            msg = "Error invoking RelValSpecMgr for file\n"
            msg += "%s\n" % relValSpecFile
            msg += str(ex)
            logging.error(msg)

        workflows = set()
        [ workflows.add(x['WorkflowSpecFile']) for x in tests ]

        for workflow in workflows:
            msg = "Publishing NewWorkflow/NewDataset for \n %s\n "% workflow
            logging.debug(msg)
            self.ms.publish("NewWorkflow", workflow)
            self.ms.publish("NewDataset", workflow)
            self.ms.commit()
            
        self.allJobs = []
        for test in tests:
            self.submitTest(test)
            
            
        msg = "Jobs Submitted:\n===============================\n"
        for j in self.allJobs:
            msg += "==> %s\n" % j
        logging.debug(msg)
        return
    

    
    def submitTest(self, test):
        """
        _submitTest_


        Submit a test by dropping the JobSpecs into the JobQueue.
        
        """
        #  //
        # // TODO:  Track each job spec ID as a RelVal job
        #//         Track each unique workflow spec
        #  //
        # // Add jobs to the JobQueue via the JobQueue API
        #//
        logging.info("RelValInjector.submitTest(%s, %s)" % (test['Name'],
                                                            test['Site']))
        
        sites = [ test['Site'] ]
        jobs = []
        for jobSpec, jobSpecFile in test['JobSpecs'].items():
            jobs.append(  {
                "JobSpecId" : jobSpec,
                "JobSpecFile" : jobSpecFile,
                "JobType" : "Processing",
                "WorkflowSpecId" : test['WorkflowSpecId'],
                "WorkflowPriority" : 100,
                
                })
            self.allJobs.append(jobSpec)
            
        bulkQueueJobs(sites, *jobs)
        
        
        
        return


        

    def startComponent(self):
        """
        _startComponent_

        Start the component and subscribe to messages

        """
        self.ms = MessageService()
        # register
        self.ms.registerAs("RelValInjector")
        
        # subscribe to messages
        self.ms.subscribeTo("RelValInjector:StartDebug")
        self.ms.subscribeTo("RelValInjector:EndDebug")
        self.ms.subscribeTo("RelValInjector:SetCurrentVersion")
        self.ms.subscribeTo("RelValInjector:SetCurrentArch")
        self.ms.subscribeTo("RelValInjector:SetCurrentCMSPath")
        self.ms.subscribeTo("RelValInjector:Inject")

        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("GeneralJobFailure")

        self.ms.subscribeTo("RelValInjector:Poll")

        #self.ms.publish("RelValInjector:Poll", "",
        #                self.args['PollInterval'])
        #self.ms.commit()
        
        while True:
            messageType, payload = self.ms.get()
            self.__call__(messageType, payload)
            self.ms.commit()
