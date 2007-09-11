#!/usr/bin/env python
"""
_DatasetFeeder_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a dataset

The input to this plugin is a workflow that contains the following
parameters:

- SplitType     event or file
- SplitSize     number of events or files per job
- InputDataset  List of InputDataset
- DBSURL        URL of DBS Instance containing the datasets

"""

import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin


class DatasetFeeder(PluginInterface):
    """
    _DatasetFeeder_

    Generate a pile of processing style jobs based on the workflow
    and dataset provided

    """
    def handleInput(self, payload):
        logging.info("DatasetFeeder: Handling %s" % payload)


registerPlugin(DatasetFeeder, DatasetFeeder.__name__)



