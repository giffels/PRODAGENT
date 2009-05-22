#!/usr/bin/env python
"""
_StoreResultsAccountantComponent_

Component that tracks StoreResults jobs in its own database and
triggers merges and export when jobs complete.

Initially based on RelValInjector

"""
import os
import time
import logging

from MessageService.MessageService import MessageService
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentDB.Config import defaultConfig as dbConfig
# from RelValInjector.RelValSpecMgr import RelValSpecMgr
from StoreResultsAccountant.ResultsStatus import ResultsStatus
from ProdCommon.Database import Session

#from JobQueue.JobQueueAPI import bulkQueueJobs

import ProdAgent.WorkflowEntities.Workflow as WEWorkflow
import ProdAgent.WorkflowEntities.Job as WEJob
import ProdAgent.WorkflowEntities.Utilities as WEUtils


class StoreResultsAccountantComponent:
    """
    _StoreResultsAccountantComponent_

    Component to trace and manage StoreResults jobs

    """
    def __init__(self, **args):
        logging.info("Trying to start StoreResultsAccountant")
        self.args = {}
        self.args['Logfile'] = None
#         self.args['FastJob'] = 250
#         self.args['MediumJob'] = 100
#         self.args['SlowJob'] = 50
#         self.args['VerySlowJob'] = 25
#         self.args['SitesList'] = None
        self.args['PollInterval'] = "00:01:00"
        self.args['MigrateToGlobal'] = False
        self.args['InjectToPhEDEx'] = False

        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        #  //
        # // Job class number of events should be ints
        #//
#         self.args['FastJob'] = int(self.args['FastJob'])
#         self.args['MediumJob'] = int(self.args['MediumJob'])
#         self.args['SlowJob'] = int(self.args['SlowJob'])
#         self.args['VerySlowJob'] = int(self.args['VerySlowJob'])
#         self.args['Fast'] = self.args['FastJob']
#         self.args['Medium'] = self.args['MediumJob']
#         self.args['Slow'] = self.args['SlowJob']
#         self.args['VerySlow'] = self.args['VerySlowJob']

        #  //
        # // List of sites to get RelVal jobs
        #//
#         self.sites = []
#         for sitename in self.args['SitesList'].split(','):
#             if len(sitename.strip()) > 0:
#                 self.sites.append(sitename.strip())

        #  //
        # // manage migration and injection
        #//
        if str(self.args['MigrateToGlobal']).lower() in ("true", "yes"):
            self.args['MigrateToGlobal'] = True
        else:
            self.args['MigrateToGlobal'] = False

        if str(self.args['InjectToPhEDEx']).lower() in ("true", "yes"):
            self.args['InjectToPhEDEx'] = True
        else:
            self.args['InjectToPhEDEx'] = False

        if self.args['MigrateToGlobal'] == False:
            # Cant inject without migration
            self.args['InjectToPhEDEx'] = False


        LoggingUtils.installLogHandler(self)
        msg = "StoreResultsAccountant Component Started:\n"
        msg += " Migrate to Global DBS: %s\n" % self.args['MigrateToGlobal']
        msg += " Inject to PhEDEx:      %s\n" % self.args['InjectToPhEDEx']
#         msg += "Jobs to be sent to Sites:\n"
#         for site in self.sites:
#             msg += " ==> %s\n" % site


        logging.info(msg)


    def __call__(self, message, payload):
        """
        _operator()_

        Respond to messages

        """
        logging.debug("Message Recieved: %s %s" % (message, payload))

        if message == "StoreResultsAccountant:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if message == "StoreResultsAccountant:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

#         if message == "StoreResultsAccountant:Inject":
#             self.inject(payload)
#             return

        if message == "StoreResultsAccountant:Poll":
            self.poll()
            return

    def poll(self):
        """
        _poll_

        Polling loop response to check status of RelVal jobs being tracked

        """
        logging.info("StoreResultsAccountant.poll()")
        #  //
        # // Poll WorkflowEntities to find all workflows owned by
        #//  this component
        relvalWorkflows = WEUtils.listWorkflowsByOwner("WorkflowInjector")
        workflows = WEWorkflow.get(relvalWorkflows)
        if type(workflows) != type(list()) :
            workflows = [workflows]
        for workflow in workflows:
            if workflow != 0:
                logging.debug(
                    "Polling for state of workflow: %s\n" % str(workflow['id']))
                status = ResultsStatus(self.args, self.ms, **workflow)
                status()

        self.ms.publish("StoreResultsAccountant:Poll", "",
                        self.args['PollInterval'])
        self.ms.commit()
        return


    def startComponent(self):
        """
        _startComponent_

        Start the component and subscribe to messages

        """
        self.ms = MessageService()
        # register
        self.ms.registerAs("StoreResultsAccountant")

        # subscribe to messages
        self.ms.subscribeTo("StoreResultsAccountant:StartDebug")
        self.ms.subscribeTo("StoreResultsAccountant:EndDebug")
        self.ms.subscribeTo("StoreResultsAccountant:Inject")

        #self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("GeneralJobFailure")

        self.ms.subscribeTo("StoreResultsAccountant:Poll")

        self.ms.publish("StoreResultsAccountant:Poll", "",
                        self.args['PollInterval'])
        self.ms.commit()

        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("StoreResultsAccountant: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()



#     def inject(self, relValSpecFile):
#         """
#         _inject_
#
#         Given the relVal spec file provided, take that and generate
#         workflows and jobs for all sites
#
#         """
#         if not os.path.exists(relValSpecFile):
#             msg = "Cannot load RelVal Spec File:\n  %s\n" % relValSpecFile
#             msg += "File does not exist..."
#             logging.error(msg)
#             return
#
#
#
#         specMgr = RelValSpecMgr(relValSpecFile, self.sites, **self.args)
#         specMgr.ms = self.ms
#
#         try:
#             tests = specMgr()
#         except Exception, ex:
#             msg = "Error invoking RelValSpecMgr for file\n"
#             msg += "%s\n" % relValSpecFile
#             msg += str(ex)
#             logging.error(msg)
#         return




##workflowIds = {}

##        [ workflowIds.__setitem__(x['WorkflowSpecId'], x['WorkflowSpecFile']) for x in tests ]

##        for workflowId, workflowFile in workflowIds.items():
##            msg = "Registering Workflow Entity: %s" % workflowId
##            logging.debug(msg)
##            WEWorkflow.register(
##                workflowId,
##                {"owner" : "RelValInjector",
##                 "workflow_spec_file" : workflowFile,

##                 })


##            msg = "Publishing NewWorkflow/NewDataset for \n"
##            msg += " %s\n "% workflowFile
##            logging.debug(msg)
##            self.ms.publish("NewWorkflow", workflowFile)
##            self.ms.publish("NewDataset", workflowFile)
##            self.ms.commit()




        ##self.allJobs = []
##        for test in tests:
##            self.submitTest(test)


##        msg = "Jobs Submitted:\n===============================\n"
##        for j in self.allJobs:
##            msg += "==> %s\n" % j
##        logging.debug(msg)
 ##       return



##    def submitTest(self, test):
##        """
##        _submitTest_


##        Submit a test by dropping the JobSpecs into the JobQueue.

##        """
##        #  //
##        # // Add jobs to the JobQueue via the JobQueue API
##        #//
##        logging.info("RelValInjector.submitTest(%s, %s)" % (test['Name'],
##                                                            test['Site']))

##        sites = [ test['Site'] ]
##        jobs = []
##        for jobSpec, jobSpecFile in test['JobSpecs'].items():
##            jobs.append(  {
##                "JobSpecId" : jobSpec,
##                "JobSpecFile" : jobSpecFile,
##                "JobType" : "Processing",
##                "WorkflowSpecId" : test['WorkflowSpecId'],
##                "WorkflowPriority" : 100,

##                })
##            self.allJobs.append(jobSpec)
##            logging.debug("Registering Job Entity: %s.%s" % (
##                test['WorkflowSpecId'], jobSpec)
##                          )
##            WEJob.register(test['WorkflowSpecId'], None, {
##                'id' : jobSpec, 'owner' : 'RelValInjector',
##                'job_type' : "Processing", "max_retries" : 3,
##                "max_racers" : 1,
##                })

##        bulkQueueJobs(sites, *jobs)



##        return




