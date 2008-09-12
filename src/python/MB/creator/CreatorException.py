#!/usr/bin/env python
# pylint: disable-msg=W0152
"""
_CreatorException_

Module containing the definitions for Creator Exceptions.

Objects: --

- *CreatorException* : Base Class for all Creator Exceptions


"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: CreatorException.py,v 1.1 2005/12/30 18:51:39 evansde Exp $"
__author__ = "evansde@fnal.gov"

from MB.MBException import MBException


class CreatorException(MBException):
    """
    Exception class for handling problems arising
    in the Creator proceedure
    """
    def __init__(self, txt, **keywords):
        MBException.__init__(self, txt, **keywords)
        


class CreationFailed(CreatorException):
    """
    Exception to be raised if the Creation attempt
    fails
    """
    def __init__(self, txt, **keywords):
        CreatorException.__init__(self, txt, **keywords)
       
