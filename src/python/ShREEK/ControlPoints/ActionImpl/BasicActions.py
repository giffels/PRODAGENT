#!/usr/bin/env python
"""
_BasicActions_

Action Implementations to
 - Kill A Job
 - Kill A Task
 - Set the next task
 
"""

import os
from ShREEK.ControlPoints.Action import Action
import ShREEK.ControlPoints.ControlPointFactory as Factory



class KillJob(Action):
    """
    _KillJob_

    Action Implementation to Kill A Job
    """
    def action(self, controlPoint):
        """
        Call the Kill Job hook from the Control
        Point.
        Echo the logfile to stdout if it exists
        """
        controlPoint.killjob()
        return



class KillTask(Action):
    """
    _KillTask_

    Action Implementation to kill a task
    """
    def action(self, controlPoint):
        """
        Call the Kill Task hook from the Control Point
        """
        controlPoint.killtask()
        return


class SetNextTask(Action):
    """
    _SetNextTask_

    Action implementation to set the next task field in the
    ExecutionManager instance

    """
    def action(self, controlPoint):
        """
        set the next task to be the content of this Action
        """
        controlPoint.setNextTask(self.content.strip())
        return
    


Factory.registerAction(KillJob)
Factory.registerAction(KillTask)
Factory.registerAction(SetNextTask)
