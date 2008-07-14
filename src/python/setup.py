#!/usr/bin/env python
"""
_ProdAgent_

Python packages for the CMS Production Agent

"""

__revision__ = "$Id: setup.py,v 1.11 2006/05/02 17:19:34 evansde Exp $"

from distutils.core import setup

packages = [
    'AdminControl',
    'CMSConfigTools',
    'DBSInterface',
    'DLSInterface',
    'ErrorHandler',
    'ErrorHandler.Handlers',
    'FwkJobRep',
    'IMProv',
    'JobCleanup',
    'JobCleanup.Handlers',
    'JobCreator',
    'JobCreator.Creators',
    'JobCreator.RuntimeTools',
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
    'MCPayloads',
    'MergeSensor',
    'MessageService',
    'ProdAgentDB',
    'ProdAgentCore',
    'RequestInjector',
    'RunRes',
    'SeedGen',
    'ShLogger',
    'ShLogger.log_adapters',
    'ShREEK',
    'ShREEK.ControlPoints',
    'ShREEK.ControlPoints.ActionImpl',
    'ShREEK.ControlPoints.CondImpl',
    'ShREEK.ShREEK_common',
    'ShREEK.CMSPlugins',
    'ShREEK.CMSPlugins.ApMon',
    'ShREEK.CMSPlugins.JobMon',
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
