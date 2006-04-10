#!/usr/bin/env python
"""
_JobSubmitter_

JobSubmitter component for ProdAgent

Handles SubmitJob Events, with the Payload being the Job Cache Area
containg the job, input sandbox etc.

On successful submission, a SubmitSuccess event is published with the
JobSpec ID as payload

On failed submission a SubmitFailure event is published with the JobSpec ID

"""

__revision__ = "$Id: __init__.py,v 1.1 2006/03/20 22:37:03 evansde Exp $"
__version__ = "$Revision: 1.1 $"

__all__ = []

import JobSubmitter.Submitters
