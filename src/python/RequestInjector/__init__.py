#!/usr/bin/env python
"""
_RequestInjector_

Placeholder for Request Retrieval and priority queue from the ProdMgr.

Contains a Workflow Definition, and generates a new job for it when
a ResourcesAvailable event is recieved. The new job is generated
as an XML MCPayload file that is made available via a URL over http.
A CreateJob event is then generated and the URL is sent as payload.

"""
__revision__ = "$Id: __init__.py,v 1.1 2005/11/22 22:21:16 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"
__all__ = []

