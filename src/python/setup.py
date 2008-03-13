#!/usr/bin/env python
"""
_ProdAgent_

Python packages for the CMS Production Agent

"""

__revision__ = "$Id: setup.py,v 1.52 2008/02/22 17:59:49 dmason Exp $"

from distutils.core import setup

packages = [
    'AdminControl',
    'AdminControl.Bots',
    'CleanUpScheduler',
    'CondorTracker',
    'CondorTracker.Trackers',
    'DBSInterface',
    'DatasetInjector',
    'ErrorHandler',
    'ErrorHandler.Handlers',
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
    'MergeSensor',
    'MergeSensor.MergePolicies',
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
    'RepackerInjector',
    'RequestInjector',
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
