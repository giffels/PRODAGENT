#!/usr/bin/env python
"""
_OfflineDQMPerRun_

Plugin for injecting a set of analysis jobs for Offline DQM tools.

These jobs are set to process the entire dataset in one job per run


"""


import logging
import pickle
import os



from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin
from ProdCommon.JobFactory.RunJobFactory import RunJobFactory


class StateFile:
    """
    _StateFile_

    pickleable state file for a particular workflow

    """
    def __init__(self):
        self.workflow = None
        self.runs = []

    def save(self, filename):
        """pickle self"""
        handle = open(filename, 'w')
        pickle.dump(self, handle)
        handle.close()
        return

    def load(self, filename):
        """unpickle self"""
        handle = open(filename, 'r')
        tmp = pickle.load(handle)
        self.workflow = tmp.workflow
        self.runs = tmp.runs
        handle.close()
        return




class OfflineDQMPerRun(PluginInterface):
    """
    _OfflineDQMPerRun_

    Plugin to generate a set of analysis jobs for a workflow
    containing an input dataset

    """
    def handleInput(self, payload):
        logging.info("OfflineDQMPerRun: Handling %s" % payload)

        self.workflow = None
        self.dbsUrl = None
        self.loadPayloads(payload)
        self.siteName = None
        self.state = StateFile()
        self.state.workflow = self.workflow.workflowName()
        self.stateFile = os.path.join(
            self.workingDir, "%s-State.pkl" % self.workflow.workflowName())

        if os.path.exists(self.stateFile):
            logging.info("State file exists for workflow: %s" % self.stateFile)
            self.state.load(self.stateFile)

        else:
            logging.info("No State file found, starting from scratch")
            self.publishWorkflow(payload, self.workflow.workflowName())
            self.publishNewDataset(payload)


        factory = RunJobFactory(self.workflow,
                                self.workingDir,
                                self.dbsUrl, SiteName = self.siteName)


        jobs = factory()

        for job in jobs:
            if job['Run'] in self.state.runs:
                msg = "Run %s already known, skipping..." % job['Run']
                logging.info(msg)
                continue
            self.queueJob(job['JobSpecId'], job['JobSpecFile'],
                          job['JobType'],
                          job['WorkflowSpecId'],
                          job['WorkflowPriority'],
                          *job['Sites'])
            self.state.runs.append(job['Run'])
        self.state.save(self.stateFile)
        return

    def loadPayloads(self, payloadFile):
        """
        _loadPayloads_


        """

        self.workflow = self.loadWorkflow(payloadFile)


        value = self.workflow.parameters.get("DBSURL", None)
        if value != None:
            self.dbsUrl = value

        if self.dbsUrl == None:
            msg = "Error: No DBSURL available for dataset:\n"
            msg += "Cant get local DBSURL and one not provided with workflow"
            logging.error(msg)
            raise RuntimeError, msg

        self.siteName = self.workflow.parameters.get("OnlySites", None)
        if self.siteName == None:
            msg = "Error: No sitename provided for OfflineDQMByRun workflow"
            msg += "\n You need to add an OnlySites parameter containing\n"
            msg += "the site name where the jobs will run"
            logging.error(msg)
            raise RuntimeError, msg

        runtimeScript = "JobCreator.RuntimeTools.RuntimeOfflineDQM"
        postTasks = self.workflow.payload.scriptControls['PostTask']
        if runtimeScript not in postTasks:
            self.workflow.payload.scriptControls['PostTask'].append(
                runtimeScript)

        return

registerPlugin(OfflineDQMPerRun, OfflineDQMPerRun.__name__)
