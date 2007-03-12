#!/usr/bin/env python
"""
_ProdAgent_

Python packages for the CMS Production Agent

"""

__revision__ = "$Id: setup.py,v 1.33 2007/03/07 22:56:59 evansde Exp $"

from distutils.core import setup

packages = [
    'AdminControl',
    'AdminControl.Bots',
    'CondorTracker',
    'CondorTracker.Trackers',
    'DBSInterface',
    'DatasetInjector',
    'ErrorHandler',
    'ErrorHandler.Handlers',
    'FwkJobRep',
    'JobCleanup',
    'JobCleanup.Handlers',
    'JobCreator',
    'JobCreator.Creators',
    'JobCreator.Generators',
    'JobCreator.RuntimeTools',
    'JobCreator.Generators',
    'JobQueue',
    'JobQueue.Prioritisers',
    'JobSubmitter',
    'JobSubmitter.Submitters',
    'JobTracking',
    'JobState',
    'JobState.JobStateAPI',
    'JobState.Database',
    'JobState.Database.Api',
    'MB',
    'MB.commandBuilder',
    'MB.dmb_tools',
    'MB.creator',
    'MB.query',
    'MB.transport',
    'MergeSensor',
    'MergeAccountant',
    'MessageService',
    'PileupTools',
    'ProdAgent',
    'ProdAgent.Core',
    'ProdAgent.Trigger',
    'ProdAgent.Trigger.Actions',
    'ProdAgent.WorkflowEntities',
    'ProdAgentBOSS',
    'ProdAgentDB',
    'ProdAgentCore',
    'ProdMgrInterface',
    'ProdMgrInterface.States',
    'ProdMgrInterface.States.Aux',
    'PhEDExInterface',
    'RequestInjector',
    'ResourceMonitor',
    'ResourceMonitor.Monitors',
    'RunRes',
    'RssFeeder',
    'SeedGen',
    'ShREEK',
    'ShREEK.ControlPoints',
    'ShREEK.ControlPoints.ActionImpl',
    'ShREEK.ControlPoints.CondImpl',
    'ShREEK.ShREEK_common',
    'ShREEK.CMSPlugins',
    'ShREEK.CMSPlugins.ApMon',
    'ShREEK.CMSPlugins.ApMonLite',
    'ShREEK.CMSPlugins.JobMon',
    'StatTracker',
    'StageOut',
    'StageOut.Impl',
    'SVSuite',
    'TaskObjects',
    'TaskObjects.Tools',
    'Trigger',
    'Trigger.Actions',
    'Trigger.Database',
    'Trigger.Database.Api',
    'Trigger.TriggerAPI',
    ]

setup(name='ProdAgent',
      version='1.0',
      description='CMS Production Agent',
      author='Dave Evans',
      author_email='evansde@fnal.gov',
      url='https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgent',
      packages=packages,
     )
