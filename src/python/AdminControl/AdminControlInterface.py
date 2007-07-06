#!/usr/bin/env python
"""
_AdminControlInterface_

XMLRPC Interface class.

Defines RPC methods for the AdminControl component to expose.

"""
import operator
import types
from MessageService.MessageService import MessageService
from MessageService.MessageServiceStatus import MessageServiceStatus
import ProdAgent.WorkflowEntities.Job as WEJobState


from xmlrpclib import Fault

_GetName = lambda x: operator.getitem(x, "Name")

class AdminControlInterface:
    """
    _AdminControlInterface_

    AdminControl XMLRPC interface object

    Expose accessor methods to retrieve information from the MessageService
    and JobState DB tables, and allow users to publish events.

    Every method should return either simple type structures (value, lists,
    dicts etc) or an xmlrpclib.Fault if there is an error
    
    """
    def __init__(self):
        self.ms = MessageService()
        self.ms.registerAs("AdminControlInterface")
        self.status = MessageServiceStatus()
        

    def publishEvent(self, eventName, payload = ""):
        """
        _publishEvent_

        Publish an event into the ProdAgent MessageService
        """
        try:
            self.ms.publish(eventName, payload)
            self.ms.commit()
            return 0
        except StandardError, ex:
            msg = "Error publishing Event: %s\n"
            msg += "Payload: %s\n"
            msg += "From AdminControl\n"
            msg += "Exception: %s" % str(ex)
            result = Fault(1, msg)
            return result

    def removePendingEvents(self, componentName):
        """
        _removePendingEvents_

        Remove all pending events from the message service for the
        component specified.

        Returns the number of events removed

        """
        try:
            ms = MessageService()
            # remove messages for component by subscribing to them as
            # that component and pulling them all out
            ms.registerAs(componentName)
            count = 0
            while True:
                type, payload = ms.get(wait = False)
                if type == None:
                    break
                else:
                    count += 1
            ms.commit()
            return count
        except StandardError, ex:
            msg = "Error while removing pending messages for:\n"
            msg += "Component Name %s\n" % componentName
            msg += str(ex)
            return Fault(1, msg)
        
 
    
    def subscribers(self):
        """
        _subscribers_

        Get details of subscribers in the system.
        Returns a list of subscriber names
        """
        try:
            return map(_GetName, self.status.listProcesses())
        except StandardError, ex:
            msg = "Error getting list of Subscribers:\n"
            msg += "%s" % ex
            return Fault(1, msg)
        
    def events(self):
        """
        _events_

        Get a list of all the events that are in the system

        """
        try:
            return map(_GetName, self.status.listEvents())
        except StandardError, ex:
            msg = "Error getting list of Events:\n"
            msg += "%s" % ex
            return Fault(1, msg)
    

    def totalPendingEvents(self, componentName = None):
        """
        _totalPendingEvents_

        Get the total number of all pending events in the ProdAgent, unless
        a component name is provided, then get only the pending events for
        that component
        
        """
        if componentName == None:
            try:
                return self.status.totalPendingMessages()
            except StandardError, ex:
                msg = "Error retrieving count of Total Pending Messages:\n"
                msg += str(ex)
                return Fault(1, msg)
        else:
            try:
                return self.status.totalPendingMessagesFor(componentName)
            except StandardError, ex:
                msg = "Error retrieving count of Pending Messages For:\n"
                msg += "Component Named %s\n" % componentName
                msg += str(ex)
                return Fault(1, msg)

    def pendingEventsFor(self, componentName, offset = None, total = None):
        """
        _pendingEventsFor_

        Get the details of the pending events for the component specified.
        Optional offset and total allow large ranges to be retrieved in
        chunks

        """
        if offset != None:
            if ((type(offset) != types.IntType) and (offset < 0)):
                msg = "Invalid Offset Argument to pendingEventsFor:\n"
                msg += "For component: %s\n" % componentName
                msg += "Offset value must be a non negative integer"
                return Fault(2, msg)
        if total != None:
            if ((type(total) != types.IntType) and (offset < 0)):
                msg = "Invalid Total Argument to pendingEventsFor:\n"
                msg += "For component: %s\n" % componentName                
                msg += "Total value must be a non negative integer"
                return Fault(2, msg)
            
        try:
            return self.status.pendingMessages(componentName, offset, total)   
        except StandardError, ex:
            msg = "Error getting pending messages for component:\n"
            msg += "Component: %s Offset: %s Total: %s\n " % (
                componentName, offset, total,
                )
            msg += str(ex)
            return Fault(1, msg)
                
        
    def totalJobSpecs(self):
        """
        _totalJobSpecs_

        Get a total number of JobSpecs being dealt with by this prodAgent

        """
        try:
            return WEJobState.jobSpecTotal()
        except StandardError, ex:
            msg = "Error retrieving JobSpec count:\n"
            msg += str(ex)
            return Fault(1, msg)

    def getJobSpecs(self, offset = -1, total = -1):
        """
        _getJobSpecs_

        Get a list of JobSpec IDs known at the prodAgent.
        Since there may be a lot of these, it is recommended that you
        use totalJobSpecs to get a count and then retrieve the IDs
        in batches using the offset and total arguments to this
        function. Not specifying these will get everything, which may
        well not be performant.

        """
        return WEJobState.rangeGeneral(offset, total)
    
        
    def purgeProdAgentDB(self):
        """
        _purgeProdAgentDB_

        Remove all pending messages from the message service and wipe out
        all JobStates information.

        Only use this method if you are sure you know what you are doing

        """
        try:
            self.ms.purgeMessages()
        except StandardError, ex:
            msg = "Failed to Purge Messages:\n"
            msg += str(ex)
            return Fault(1, msg)
        try:
            WEJobState.purgeStates()
        except StandardError, ex:
            msg = "Failed to Purge States:\n"
            msg += str(ex)
            return Fault(1, msg)
        return 0

        
