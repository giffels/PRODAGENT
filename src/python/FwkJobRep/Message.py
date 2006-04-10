#!/usr/bin/env python
"""
_Message_

Simple container for representing plain text messages from a framework
job report

"""

from IMProv.IMProvNode import IMProvNode


class Message(dict):
    """
    _Message_

    Messages from the Framework along with a severity and some other info.
    Mirrors the structure produced by the FwkJob Report Message Logger
    destination.

    

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Severity", None)
        self.setdefault("Message", [])
        self.setdefault("Module", "")


    def __str__(self):
        """string representation"""
        result = "Message: Severity: %s Module: %s\n" % (
            self["Severity"], self['Module'],
            )
        for item in self['Message']:
            result += "%s\n" % item
        return result



    def save(self):
        """
        _save_

        Write out this object in the standard FwkJob Report format

        """
        result = IMProvNode("Report")
        result.addNode(IMProvNode("Severity", self['Severity']))
        result.addNode(IMProvNode("Category", "FwkJob"))
        message = IMProvNode("Message")
        result.addNode(message)
        for item in self['Message']:
            message.addNode(IMProvNode("Item", item))
            
        result.addNode(IMProvNode("Module", self['Module']))
        return result

    
                       
                           

