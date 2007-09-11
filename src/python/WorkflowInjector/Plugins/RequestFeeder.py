#!/usr/bin/env python
"""
_RequestFeeder_

Plugin to generate a fixed amount of production jobs from a workflow.

The input to this plugin is a workflow that contains the following
parameters:

- NumberOfEvents
- EventsPerJob

"""

import logging

from WorkflowInjector.PluginInterface import PluginInterface
from WorkflowInjector.Registry import registerPlugin


class RequestFeeder(PluginInterface):
    """
    _RequestFeeder_

    Generate a pile of production style jobs based on the workflow
    provided

    """
    def handleInput(self, payload):
        logging.info("RequestFeeder: Handling %s" % payload)


registerPlugin(RequestFeeder, RequestFeeder.__name__)



