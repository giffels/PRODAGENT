#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_TransportException_

Module containing the definitions for Transport Exceptions.

Objects: --

- *TransportException* : Base Class for all Transport Exceptions


"""
__version__ = "$Version$"
__revision__ = \
  "$Id: TransportException.py,v 1.1 2005/12/30 18:51:41 evansde Exp $"

from MB.MBException import MBException


class TransportException(MBException):
    """
    Exception class for handling problems arising
    in the Transport proceedure
    """
    def __init__(self, txt, **keywords):
        MBException.__init__(self, txt, **keywords)
        


class TransportFailed(TransportException):
    """
    Exception to be raised if the Transport attempt
    fails
    """
    def __init__(self, txt, **keywords):
        TransportException.__init__(self, txt, **keywords)
       
