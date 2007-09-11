#!/usr/bin/env python
"""
_DatasetScrambler_

Plugin to generate a fixed amount of production jobs from a workflow that
processes a set of datasets and mixes the injection of the
jobs in a random manner

The input to this plugin is a workflow that contains the following
parameters:

- SplitType     event or file
- SplitSize     number of events or files per job
- InputDatasets  List of InputDatasets 
- DBSURL        URL of DBS Instance containing the datasets

"""

import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin


class DatasetScrambler(PluginInterface):
    """
    _DatasetScrambler_

    Generate a pile of processing style jobs based on the workflow
    provided using a list of input datasets and mixing the datasets
    together.

    """
    def handleInput(self, payload):
        logging.info("DatasetScrambler: Handling %s" % payload)




registerPlugin(DatasetScrambler, DatasetScrambler.__name__)



