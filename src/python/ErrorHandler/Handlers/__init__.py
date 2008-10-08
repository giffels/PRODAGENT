#!/usr/bin/env python

"""
This module contains the various handlers that can be called when receiving an
error event. The some handlers are composite, in that events might first go 
through one handler and then through another.

Error handling propagation hierarchy (this not inheritance!!)

ErrorHandler Component
         |
         |-->RunFailureHandler
         |       |
         |       |-->MergeRunFailureHandler      
         |       |
         |       |-->ProcessingRunFailureHandler
         |
         |-->SubmitFailureHandler
         |       |
         |       |-->MergeSubmitFailureHandler   
         |       |
         |       |-->ProcessingSubmitFailureHandler
         |
         |-->CreateFailureHandler

"""

import RunFailureHandler
import MergingRunFailureHandler
import ProcessingRunFailureHandler
import SubmitFailureHandler
import MergingSubmitFailureHandler
import ProcessingSubmitFailureHandler
import CreateFailureHandler

## CrabServer plugin
try:
    import Plugins.ErrorHandler.CrabRunFailureHandler
except ImportError:
    pass
