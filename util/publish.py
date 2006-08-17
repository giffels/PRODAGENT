#!/usr/bin/env python
"""
_publish_

Simple Util to publish an event into the ProdAgent via the Message Service.

The ProdAgent envionment including PYTHONPATH and PRODAGENT_CONFIG need to
be set to use this tool

"""

__version__ = "$Revision$"
__revision__ = "$Id$"

import sys

if len(sys.argv) not in  ( 2, 3,):
    print "Usage:  publish.py <event> <payload>"
    print "        <event> - required - name of event to publish"
    print "        <payload> - optional - content of event payload"
    sys.exit(0)

event = sys.argv[1]
payload = None
if len(sys.argv) > 2:
    payload = sys.argv[2]



from MessageService.MessageService import MessageService
ms = MessageService()
ms.registerAs("Test")


if payload != None:
    ms.publish(event, payload)
else:
    ms.publish(event, "")
ms.commit()

