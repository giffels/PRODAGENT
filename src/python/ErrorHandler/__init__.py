#!/user/bin/env python

"""
_ErrorHandler_

Component that handles errors, by subscribing
to error events. Depending on the type of error
(e.g. job run error) and the type
of job (e.g. merge, processing,...) it will 
initiate the appropiate error handler and update
the jobstate. As a result it will create either 
a submit event (to re submit the job) or a general
failure event (maximum number of submission is reached).

The error handler has a pluggable structure in which
also other (non job related error handlers can 
be inserted and configured). Upon receiving the event it will
activate the proper error handler and pass the payload
to this handler using the HandlerInterface method:
handlerError(payload).

"""
__revision__ = "$Id: __init__.py,v 1.3 2006/03/24 19:20:30 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "fvlingen@caltech.edu"

import ErrorHandler.Handlers
