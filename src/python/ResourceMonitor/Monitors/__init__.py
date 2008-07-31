#!/usr/bin/env python
"""
_ResourceMonitor.Monitors_

Plugins for ResourceMonitor

"""
__all__ = []

import TestMonitors
import CondorMonitor
import GlideinWMSMonitor
import T0LSFMonitor
import SimpleMonitors
import ARCMonitor
try:
    import LCGAdvanced
except:
    pass
try:
    import BlSimpleMonitors
except:
    pass