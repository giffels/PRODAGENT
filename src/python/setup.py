#!/usr/bin/env python
"""
_ProdAgent_

Python packages for the CMS Production Agent

"""

__revision__ = "$Id: setup.py,v 1.57 2008/07/25 15:29:17 swakef Exp $"

from distutils.core import setup

packages = [
    'AdminControl',
    'AdminControl.Bots',
    'AlertHandler',
    'AlertHandler.Handlers',
    'CleanUpScheduler',
    'CondorTracker',
    'CondorTracker.Trackers',
    'DBSInterface',
    'ErrorHandler',
    'ErrorHandler.Handlers',
    'GetOutput',
    'HTTPFrontend',
    'JobCleanup',
    'JobCleanup.Handlers',
    'JobCreator',
    'JobCreator.Creators',
    'JobCreator.Generators',
    'JobCreator.RuntimeTools',
    'JobCreator.Generators',
    'JobEmulator',
    'JobEmulator.JobAllocationPlugins',
    'JobEmulator.JobCompletionPlugins',
    'JobEmulator.JobReportPlugins',
    'JobQueue',
    'JobQueue.Prioritisers',
    'JobKiller',
    'JobKiller.Killers',
    'JobSubmitter',
    'JobSubmitter.Submitters',
    'JobTracking',
    'JobState',
    'JobState.JobStateAPI',
    'JobState.Database',
    'JobState.Database.Api',
    'LogCollector',
    'MergeSensor',
    'MergeSensor.MergePolicies',
    'MergeSensor.MergeSensorDB',
    'MergeSensor.MergeSensorDB.Interface',
    'MergeSensor.MergeSensorDB.MySQL',
    'MergeSensor.MergeSensorDB.MySQL.Dataset',  
    'MergeSensor.MergeSensorDB.MySQL.File',
    'MergeSensor.MergeSensorDB.MySQL.Job',
    'MergeSensor.MergeSensorDB.MySQL.Schema',
    'MergeSensor.MergeSensorDB.MySQL.Status',
    'MergeSensor.MergeSensorDB.MySQL.Workflow',
    'MergeAccountant',
    'MessageService',
    'PileupTools',
    'ProdAgent',
    'ProdAgent.Core',
    'ProdAgent.Resources',
    'ProdAgent.ResourceControl',
    'ProdAgent.Trigger',
    'ProdAgent.Trigger.Actions',
    'ProdAgent.WorkflowEntities',
    'ProdAgentBOSS',
    'ProdAgentDB',
    'ProdAgentCore',
    'ProdMgrInterface',
    'ProdMgrInterface.States',
    'ProdMgrInterface.States.Aux',
    'ProdMon',
    'RelValInjector',
    'ResourceMonitor',
    'ResourceMonitor.Monitors',
    'RssFeeder',
    'ShREEK',
    'ShREEK.ControlPoints',
    'ShREEK.ControlPoints.ActionImpl',
    'ShREEK.ControlPoints.CondImpl',
    'ShREEK.ShREEK_common',
    'ShREEK.CMSPlugins',
    'ShREEK.CMSPlugins.ApMon',
    'ShREEK.CMSPlugins.ApMonLite',
    'ShREEK.CMSPlugins.JobMon',
    'StageOut',
    'StageOut.Impl',
    'TaskObjects',
    'TaskObjects.Tools',
    'Trigger',
    'Trigger.Actions',
    'Trigger.Database',
    'Trigger.Database.Api',
    'Trigger.TriggerAPI',
    'WorkflowInjector',
    'WorkflowInjector.Plugins',

    ]

setup(name='ProdAgent',
      version='1.0',
      description='CMS Production Agent',
      author='Dave Evans',
      author_email='evansde@fnal.gov',
      url='https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgent',
      packages=packages,
     )
