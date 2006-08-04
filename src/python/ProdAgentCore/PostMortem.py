#!/usr/bin/env python
"""
_PostMortem_

Wrapper util for components so that any nasty crashes can be logged

"""

import os
import sys
import time
import traceback

def runWithPostMortem(componentRef, componentDir ):
    """
    _runWithPostMortem_

    Invoke the componentRef.startComponent method in a general
    exception catching block and catch and report any component
    crashes to a logfile in that components dir.

    """
    
    try:    
        componentRef.startComponent()
    except Exception, ex:
        errLog = os.path.join(componentDir,
                              "PostMortem-%s" % time.time())
        
        handle = open(errLog, 'w')
        handle.write("Uncaught Exception from Component:\n")
        handle.write("Exception: %s \n" % ex )
        traceback.print_tb(sys.exc_info()[2], None, handle)
        handle.close()
        sys.exit(1)
        
