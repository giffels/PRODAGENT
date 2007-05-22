#!/usr/bin/env python
"""
_KillerInterface_

Standard Interface for JobKiller plugins


"""

import logging

class KillerInterface:


    def __init__(self, args):
        pass


    
    def killJob(self, jobSpecID):
        """
        _killJob_
        
        Kill a single job with the jobspec ID provided
        
        """
        msg = "Not Implemented: %s.killJob %s" % (
             self.__class__.__name__ ,
             jobSpecId)
        logging.warning(msg)
        raise NotImplementedError, msg


    def killWorkflow(self, workflowSpecId):
        """
        _killWorkflow_

        Kill all jobs associated wit the provided workflow spec ID

        """
        msg = "Not Implemented: %s.killWorkflow %s" % (
             self.__class__.__name__ ,
             workflowSpecId)
        logging.warning(msg)
        raise NotImplementedError, msg


    def eraseJob(self, jobSpecId):
        """
        _eraseJob_

        Kill a job and remove all details from PA without causing
        JobFailure events

        """
        msg = "Not Implemented: %s.eraseJob %s" % (
             self.__class__.__name__ ,
             jobSpecId)
        logging.warning(msg)
        raise NotImplementedError, msg

    def eraseWorkflow(self, workflowSpecId):
        """
        _eraseWorkflow_

        Erase all jobs associated with the provided workflow spec ID
        and remove all details from PA without causing JobFailure events
        
        """
        msg = "Not Implemented: %s.eraseWorkflow %s" % (
             self.__class__.__name__ ,
             workflowSpecId)
        logging.warning(msg)
        raise NotImplementedError, msg

    def killTask(self, taskSpecId):
        """
        _killTask_

        Kill all jobs with the taskspec ID provided

        """
        msg = "Not Implemented: %s.killTask %s" % (
             self.__class__.__name__ ,
             taskSpecId)
        logging.warning(msg)
        raise NotImplementedError, msg

        
