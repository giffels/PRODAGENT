#!/usr/bin/env python
"""
_MessageServiceStatus_

API for doing read only queries to get details of the MessageService
status

"""
__author__ = "evansde@fnal.gov"


import operator
import MySQLdb

from ProdAgentDB.Connect import connect

#  //
# // Lambda for use in fast unpacking of DB rows from queries.
#//  
_Convert = lambda x: operator.getitem(x, 0)


class MessageServiceStatus:
    """
    _MessageServiceStatus_

    Collection of tools for reading the state of the message service.
    To be used for monitoring the state of the prodAgent etc.

    """
    def __init__(self):
        pass
    


    def listProcesses(self):
        """
        _listSubscribers_

        Generate a list of registered processes that are
        publishing/subscribing

        returns a list of dictionaries containing the data for each
        process
        """
        connection = connect()
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        sqlStr = """
           SELECT procid AS ID, name AS Name, host AS Host, pid AS Process
             FROM ms_process;
           """
        cursor.execute(sqlStr)
        result = cursor.fetchall()
        cursor.close()
        return list(result)


    def listEvents(self):
        """
        _listEvents_

        List the currently known events.
        Returns a list of dictionaries containing name and id of each event

        """
        connection = connect()
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        sqlStr = """
           SELECT typeid AS ID, name AS Name  FROM ms_type;
           """

        cursor.execute(sqlStr)
        result = cursor.fetchall()
        cursor.close()
        return result

        
    def isSubscribedTo(self, eventName):
        """
        _isSubscribedTo_

        Return a list of process names that are subscribed to the
        event name provided

        """

        sqlStr = """

        SELECT ms_process.name FROM ms_process, ms_subscription, ms_type
          WHERE ms_type.name="%s"
            AND ms_subscription.typeid=ms_type.typeid
            AND ms_subscription.procid=ms_process.procid;

        """ % eventName
    
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sqlStr)
        result = map(_Convert, cursor.fetchall())
        cursor.close()
        return result

    def subscribedToEvents(self, processName):
        """
        _subscribedToEvents_

        Return a list of events that the processName provided is subscribed
        to.

        """
        sqlStr = """

        SELECT ms_type.name FROM ms_type, ms_subscription, ms_process
          WHERE ms_process.name="%s"
            AND ms_subscription.procid=ms_process.procid
            AND ms_subscription.typeid=ms_type.typeid;
        """ % processName
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sqlStr)
        result = map(_Convert, cursor.fetchall())
        cursor.close()
        return result

    def pendingMessagesCount(self, subscriberName):
        """
        _pendingMessageCount_

        count the number of pending messages for the subscriber
        specified
        
        """
        sqlStr = \
        """
         SELECT COUNT(*) 
            FROM ms_message, ms_type, ms_subscription, ms_process
              WHERE ms_process.name="%s"
                AND ms_type.typeid=ms_message.type
                AND ms_subscription.procid=ms_process.procid
                AND ms_subscription.typeid=ms_message.type;

        """ % subscriberName
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sqlStr)
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def pendingMessages(self, subscriberName, offset = None, total = None):
        """
        _pendingMessages_

        Get a list of messages that are pending in the queue for the
        subscriber name provided

        """
        sqlStr = \
        """
         SELECT ms_message.messageid AS MessageID,
                ms_type.name AS MessageName,
                ms_message.payload AS MessagePayload
            FROM ms_message, ms_type, ms_subscription, ms_process
              WHERE ms_process.name="%s"
                AND ms_type.typeid=ms_message.type
                AND ms_subscription.procid=ms_process.procid
                AND ms_subscription.typeid=ms_message.type
        """ % subscriberName

        
        if (offset == None) and (total == None):
            sqlStr += ";"

        if (offset != None) and (total != None):
            sqlStr += " LIMIT %s, %s; " % (offset, total)
        
        
        connection = connect()
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sqlStr)
        result = cursor.fetchall()
        cursor.close()
        return result
        
    def totalPendingMessages(self):
        """
        _totalPendingMessages_

        Get the total count of all currently pending messages

        """
        sqlStr = \
        """
            SELECT COUNT(messageid) FROM ms_message;
        """
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sqlStr)
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def totalPendingMessagesFor(self, subscriber):
        """
        _totalPendingMessagesFor_

        Get the count of all pending messages for the subscriber
        name provided

        """
        sqlStr = \
        """
         SELECT COUNT(*)
           FROM ms_message, ms_type, ms_subscription, ms_process
              WHERE ms_process.name="%s"
                AND ms_type.typeid=ms_message.type
                AND ms_subscription.procid=ms_process.procid
                AND ms_subscription.typeid=ms_message.type;

        """ % subscriber
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sqlStr)
        result = cursor.fetchone()[0]
        cursor.close()
        return result
    
