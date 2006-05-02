#!/usr/bin/env python
"""
_ProdAgent_

Python packages for the CMS Production Agent

"""

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
    'TaskObjects.Tools'
    ]

setup(name='ProdAgent',
      version='1.0',
      description='CMS Production Agent',
      author='Dave Evans',
      author_email='evansde@fnal.gov',
      url='https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgent',
      packages=packages,
     )
