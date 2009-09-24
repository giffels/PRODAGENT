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
import traceback

import ProdAgentCore.LoggingUtils           as LoggingUtils
import ProdAgent.WorkflowEntities.Job       as WEJob
import ProdAgent.WorkflowEntities.Utilities as WEUtils
import ProdAgent.WorkflowEntities.Workflow  as WEWorkflow

from ProdAgentDB.Config                   import defaultConfig as dbConfig
from ProdAgentCore.Configuration          import loadProdAgentConfiguration

from WMCore.Services.PhEDEx.PhEDEx                       import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList

from MessageService.MessageService        import MessageService
from ProdCommon.Database                  import Session
from ProdCommon.MCPayloads.WorkflowSpec   import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSReader    import DBSReader
from StoreResultsAccountant.ResultsStatus import ResultsStatus

def getGlobalDBSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dbsConfig = config.getConfig("GlobalDBSDLS")
    except StandardError, ex:
        msg = "Error reading configuration for GlobalDBSDLS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dbsConfig.get("DBSURL", None)



def getPhedexDSURL():
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dsConfig = config.getConfig("PhEDExDataserviceConfig")
    except StandardError, ex:
        msg = "Error reading configuration for PhEDExDataservice:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    return dsConfig.get("DataserviceURL", None)



class StoreResultsAccountantComponent:
    """
    _StoreResultsAccountantComponent_

    Component to trace and manage StoreResults jobs

    """
    def __init__(self, **args):
        logging.info("Trying to start StoreResultsAccountant")
        self.args = {}
        self.args['Logfile'] = None
        self.args['PollInterval'] = "00:02:00"
        self.args['MigrateToGlobal'] = True
        self.args['InjectToPhEDEx'] = True

        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

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

        if message == "StoreResultsAccountant:Poll":
            try:
                self.poll()
                return
            except StandardError, ex:
                logging.error("Failed to Poll: %s" % payload)
                msg =  traceback.format_exc()
                logging.error("Details: \n%s" % msg)
                return

        if message == "PhEDExDataServiceInject":
            try:
                self.phedexInjectDataset(payload)
                return
            except StandardError, ex:
                logging.error("Failed to Inject: %s" % payload)
                msg =  traceback.format_exc()
                logging.error("Details: \n%s" % msg)
                return


    def poll(self):
        """
        _poll_

        Polling loop response to check status of RelVal jobs being tracked

        """
        #  //
        # // Poll WorkflowEntities to find all workflows owned by
        #//  this component
        relvalWorkflows = WEUtils.listWorkflowsByOwner("WorkflowInjector")
        workflows = WEWorkflow.get(relvalWorkflows)
        if type(workflows) != type(list()) :
            workflows = [workflows]
        logging.info("StoreResultsAccountant.poll() checking %s workflows" % len(workflows))
        for workflow in workflows:
            if workflow != 0:
                logging.debug(
                    "Polling for state of workflow: %s" % str(workflow['id']))
                status = ResultsStatus(self.args, self.ms, **workflow)
                status()

        self.ms.publishUnique("StoreResultsAccountant:Poll", "",
                        self.args['PollInterval'])
        self.ms.commit()
        return


    def phedexInjectDataset(self, payload):
        """
        inject a dataset into Phedex using the dataservice
        """
        dbsURL = getGlobalDBSURL()
        dsURL  = getPhedexDSURL()
        spec   = WorkflowSpec()
        try:
            spec.load(payload)
        except Exception, ex:
            msg = "Unable to read WorkflowSpec file:\n"
            msg += "%s\n" % payload
            msg += str(ex)
            logging.error(msg)
            msg =  traceback.format_exc()
            logging.error("Details: \n%s" % msg)
            return

        datasetName = '/%s/%s/USER' % \
             (spec.payload._OutputDatasets[0]['PrimaryDataset'],
              spec.payload._OutputDatasets[0]['ProcessedDataset'])
        phedexGroup = spec.payload._OutputDatasets[0]['PhysicsGroup']
        injectNode = spec.parameters['InjectionNode']
        destNode = spec.parameters['SubscriptionNode']

        logging.info("Injecting dataset %s at: %s" % (datasetName,injectNode))

        peDict = {'endpoint':dsURL}
        phedexAPI = PhEDEx(peDict)
        reader = DBSReader(dbsURL)

        blocks = reader.dbs.listBlocks(dataset = datasetName)
        blockNames = []

        for block in blocks:
            blockNames.append(block['Name'])

        jsonOutput = phedexAPI.injectBlocks(dbsURL, injectNode, datasetName, 0 , 1, *blockNames)
        logging.info("Injection results: %s" % jsonOutput)

        sub = PhEDExSubscription(datasetName, destNode, phedexGroup)
        #logging.info("Subscribing dataset to: %s" % destNode)
        subList = SubscriptionList()
        subList.addSubscription(sub)
        for sub in subList.getSubscriptionList():
            jsonOutput = phedexAPI.subscribe(dbsURL, sub)
            #logging.info("Subscription results: %s" % jsonOutput)

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

        #self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("GeneralJobFailure")

        self.ms.subscribeTo("PhEDExDataServiceInject")
        self.ms.subscribeTo("StoreResultsAccountant:Poll")

        self.ms.remove("StoreResultsAccountant:Poll")
        self.ms.publishUnique("StoreResultsAccountant:Poll", "",
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
