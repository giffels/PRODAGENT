#!/usr/bin/env python
"""
_FileListFeeder_

Plugin to generate a set of processing jobs with a given workflow
for a predefined list of files

The input to this plugin is a workflow that contains the following
parameters:

- SplitType
- SplitSize
- InputFiles

NOTE: All Input Dataset information is extracted from the workflow
spec provided.

"""

import logging


from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin


class FileListFeeder(PluginInterface):
    """
    _FileListFeeder_

    Generate a pile of processing style jobs based on the workflow
    provided and a list of files assumed to be a subset of the input
    dataset

    """
    def handleInput(self, payload):
        logging.info("FileListFeeder: Handling %s" % payload)

        

registerPlugin(FileListFeeder, FileListFeeder.__name__)



