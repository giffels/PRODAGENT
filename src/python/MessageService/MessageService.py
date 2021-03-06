#!/usr/bin/env python
"""
_MessageService_

This module implements the message service for inter-component communication
in the Production Agent. The message service follows the standard and well
known message passing approach, with the main objective being the reliable
delivery of messages between components. The message service is defined to
provide asynchronous delivery of messages, persistence and transaction
support.

"""

__revision__ = "$Id: MessageService.py,v 1.11 2008/02/04 15:30:30 swakef Exp $"
__version__ = "$Revision: 1.11 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import time
import os
import socket
import logging

from ProdAgentDB.Connect import connect

##############################################################################
# Message Service class
##############################################################################

class MessageService:
    """
    _MessageService_

    A message in the context of the message service is a structured data object
    that consists of a type and a payload. Components register in the message
    service by providing a string to be used as a component identifier. Through
    a subscription process, the components express their interest in getting
    messages of specific types. Components send messages (publish operation) by
    specifying both the type and the payload. Every time a component asks for a
    message (get operation) the message service returns the oldest not yet
    delivered message of any of the subscribed types.

    Database connections are refreshed (or a chance to do it is given) after
    a transaction is comitted or rolledback, and after the execution of
    methods that are a complete transaction by themselves.


    An attempt is performed to recover interrupted transactions (after a
    database connection lost for example) by performing again all operations
    in failed transactions. If the process fails, the component can be
    restarted and an automatic roll back process that ensures integrity will
    take place.

    """
    
    ##########################################################################
    # Message Server class initialization
    ##########################################################################

    def __init__(self):
        """
        __init__
        
        Open a connection to the messages database and set initial parameters.
        Database information can be obtained at the Message Service twiki
        page:
                       
           https://uimon.cern.ch/twiki/bin/view/CMS/ProdAgentLiteMessageService
            
        Following the link on section "The Database specification"
        """

        # logging
        logging.getLogger()
        logging.debug("MS: initializing")
        
        # initialize internal variables
        self.name = None
        self.procid = None
        self.transaction = []

        # parameters
        self.refreshPeriod = 60 * 60 * 12
        self.pollTime = 5

        # force open connection
        self.connectionTime = 0
        self.conn = self.connect(invalidate = True)
        
        # logging
        logging.debug("MS: initialization OK")

    ##########################################################################
    # register method 
    ##########################################################################

    def registerAs(self, name):
        """
        __registerAs__
        
        The operation registerAs registers the component as 'name' in the
        message service, including information on the host name and its PID.
        It is assumed that only one process with the same name is running in
        the production agent. Attempt to register a process with the same
        name will result in an update of its hostname and PID, since it is
        assumed that the old process crashed and it was started again.
        """
        
        # logging
        logging.debug("MS: registerAs requested")

        # set component name
        self.name = name
        
        # get process data
        currentPid = os.getpid()
        currentHost = socket.gethostname()
        
        # open connection
        self.conn = self.connect()
        cursor = self.conn.cursor()
                                                                                
        # check if process name is in database
        sqlCommand = """
                     SELECT procid, host, pid
                       FROM ms_process
                       WHERE name = '""" + name + """'
                     """
        cursor.execute(sqlCommand)
        rows = cursor.rowcount

        # process was registered before
        if rows == 1:
            
            # get data
            row = cursor.fetchone()
            procid, host, pid = row
            
            # if pid and host are the same, get id and return
            if host == currentHost and pid == currentPid:
                self.procid = procid
                cursor.close()
                return
            
            # process was replaced, update info
            else:
                sqlCommand = """
                         UPDATE
                             ms_process
                           SET
                             pid = '"""+ str(currentPid) + """',
                             host = '"""+ currentHost + """'
                           WHERE name = '""" + name + """'
                         """
                cursor.execute(sqlCommand)
                self.transaction.append(sqlCommand)
                cursor.close()
                self.commit()
                self.procid = procid
                return
                
        # register new process in database
        sqlCommand = """
                     INSERT
                       INTO ms_process
                         (name, host, pid)
                       VALUES
                         ('""" + name + """',
                         '""" + currentHost + """',
                         '""" + str(currentPid) + """')
                     """
        cursor.execute(sqlCommand)
        self.transaction.append(sqlCommand)

        # get id
        sqlCommand = "SELECT LAST_INSERT_ID()"
        cursor.execute(sqlCommand)
        row = cursor.fetchone()
        self.procid = row[0]
        
        # return
        cursor.close()
        self.commit()

    ##########################################################################
    # subscribe method
    ##########################################################################

    def subscribeTo(self, name):
        """
        __subscribeTo__
        
        The operation subscribeTo subscribes the current component to messages
        of type 'name'. 
        
        The message type is registered in the database if it was not
        registered before.
        """

        # logging
        logging.debug("MS: subscribeTo requested")

        # open connection       
        self.conn = self.connect()
        cursor = self.conn.cursor()
 
        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        cursor.execute(sqlCommand)
        rows = cursor.rowcount

        # get message type id
        if rows == 1:
            
            # message type was registered before, get id
            row = cursor.fetchone()
            typeid = row[0]
            
        else:

            # not registered before, so register now
            sqlCommand = """
                         INSERT
                           INTO ms_type
                             (name)
                           VALUES
                             ('""" + name + """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            
            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            cursor.execute(sqlCommand)
            row = cursor.fetchone()
            typeid = row[0]

        # check if there is an entry in subscription table
        sqlCommand = """
                     SELECT procid, typeid
                       FROM ms_subscription
                       WHERE procid = '""" + str(self.procid) + """'
                         AND typeid = '""" + str(typeid) + """'
                     """
        cursor.execute(sqlCommand)
        rows = cursor.rowcount

        # entry registered before, just return
        if rows == 1:
            cursor.close()
            return
        
        # not registered, do it now
        sqlCommand = """
                     INSERT
                       INTO ms_subscription
                         (procid, typeid)
                       VALUES ('""" + str(self.procid) + """',
                               '""" + str(typeid) + """')
                     """
        cursor.execute(sqlCommand)
        self.transaction.append(sqlCommand)

        # return
        cursor.close()
        self.commit()

    ##########################################################################
    # publish method 
    ##########################################################################

    def publish(self, name, payload, delay="00:00:00", cursor=None):
        """
        _publish_
        
        The operation publish sends the message of type 'name' with content
        specified by 'payload' to all components subscribed to this message
        type.

        The message type is registered in the database if it was not
        registered before.

        Returns the number of destinations to where the message was delivered.
        """
        
        # logging
        logging.debug("MS: publish requested")

        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
                     
        cursor = self.executeSQLwithRetry(sqlCommand, cursor)

        rows = cursor.rowcount

        # get message type id
        if rows == 1:
            
            # message type was registered before, get id
            row = cursor.fetchone()
            typeid = row[0]
            
        else:

            # not registered before, so register now
            sqlCommand = """
                         INSERT
                           INTO ms_type
                             (name)
                           VALUES
                             ('""" + name + """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)

            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            cursor.execute(sqlCommand)
            row = cursor.fetchone()
            typeid = row[0]
            
        # get destinations
        sqlCommand = """
                     SELECT procid
                       FROM ms_subscription
                       WHERE typeid = '""" + str(typeid) + """'
                     """
        cursor.execute(sqlCommand)
        
        destinations = self.getList(cursor)
        
        # add message to database for delivery
        destCount = 0;
        for dest in destinations:
            sqlCommand = """
                         INSERT
                           INTO ms_message
                             (type, source, dest, payload,delay)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """',
                                   '""" + str(delay)+ """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            sqlCommand = """
                         INSERT
                           INTO ms_history
                             (type, source, dest, payload,delay)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """',
                                   '""" + str(delay)+ """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            destCount += 1

        # return
        cursor.close()
        return destCount
    
    
    def publishUnique(self, name, payload, delay="00:00:00", cursor=None):
        """
            publish method that only publishes if no
            messages of the same type type exist
        """
        
        # logging
        logging.debug("MS: publishUnique requested")

        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        cursor = self.executeSQLwithRetry(sqlCommand, cursor)

        rows = cursor.rowcount

        if rows == 0:
            # not registered before, so cant have any instances
            return self.publish(name, payload, delay, cursor)
        
        # message type was registered before, get id
        row = cursor.fetchone()
        typeid = row[0]
                        
        # message known - how many in queue?
        sqlCommand = """
                     SELECT COUNT(*)
                       FROM ms_message
                       WHERE type = '""" + str(typeid) + """'
                     """
    
        cursor.execute(sqlCommand)
        
        num = cursor.fetchone()[0]
        
        if num == 0:
            # no messages - so publish
            return self.publish(name, payload, delay, cursor)
        
        # message exists - do not publish another
        cursor.close()
        return 0
    


    ##########################################################################
    # get method 
    ##########################################################################

    def get(self, wait = True):
        """
        __get__
        
        The operatios get returns both the type and the payload of a single
        message.
        
        Polling is performed in this prototype implementation to wait for new
        messages.
        """

        # logging
        logging.debug("MS: get requested")

        # get messages command
        sqlCommand = """
                     SELECT messageid, name, payload
                       FROM ms_message, ms_type
                       WHERE
                         typeid=type and
                         ADDTIME(time,delay) <= CURRENT_TIMESTAMP and
                         dest='""" + str(self.procid) + """'
                       ORDER BY time,messageid
                       LIMIT 1
                       
                     """

        # check for messages
        while True:

            # get messsages
            try:
                # get cursor
                cursor = self.conn.cursor()

                # execute command
                cursor.execute(sqlCommand)
            except:

                # logging
                logging.warning("MS: connection to database lost")

                # if it does not work, we lost connection to database.
                self.conn = self.connect(invalidate = True)
                                                                                
                # logging
                logging.warning("MS: connection to database recovered")

                # redo operations in interrupted transaction
                self.redo()
                                                                                
                # get cursor
                cursor = self.conn.cursor()
                                                                                
                # retry
                cursor.execute(sqlCommand)

            # there is one, return it
            if cursor.rowcount == 1:
                break

            # close cursor
            cursor.close()
                
            # no messages yet
            if not wait:

                # return immediately with no message
                return (None, None)

            # or wait and try again after some time
            time.sleep(self.pollTime)      
 
        # get data
        row = cursor.fetchone()
        messageid, type, payload = row
        
        # remove messsage
        sqlCommand = """
                     DELETE 
                       FROM ms_message
                       WHERE
                         messageid='""" + str(messageid) + """'
                       LIMIT 1
                     """
        cursor.execute(sqlCommand)
        self.transaction.append(sqlCommand)

        # return message
        cursor.close()
        return (type, payload)

    ##########################################################################
    # commit method 
    ##########################################################################

    def commit(self):
        """
        __commit__
        
        The operation commit closes the current transaction, making all
        message operations to take place as a single atomic operation.
        """

        # logging
        logging.debug("MS: commit requested")

        # commit
        try:
            self.conn.commit()
        except:

            # logging
            logging.warning("MS: connection to database lost")

            # lost connection with database, reopen it
            self.conn = self.connect(invalidate = True)

            # logging
            logging.warning("MS: connection to database recovered")

            # redo operations in interrupted transaction
            self.redo()

            # try to commit
            self.conn.commit()

        # erase redo list
        self.transaction = []

        # refresh connection
        self.conn = self.connect()

    ##########################################################################
    # rollback method 
    ##########################################################################

    def rollback(self):
        """
        __rollback__
        
        The operation rollback discards all message related operations in the
        current transaction. 
        """

        # logging
        logging.debug("MS: rollback requested")

        # roll back
        try:
            self.conn.rollback()
        except:
            # lost connection con database, just get a new connection
            # the effect of rollback is then automatic

            # logging
            logging.warning("MS: connection to database lost")

            pass

        # erase redo list
        self.transaction = []

        # refresh connection
        self.conn = self.connect()

    ##########################################################################
    # redo method
    ##########################################################################
                                                                                
    def redo(self):
        """
        __redo__
        
        Tries to redo all operations pending (uncomitted) performed during
        an interrupted transaction.

        If it cannot be done, the component can safely be restarted and
        transaction will be automatically rolled back
        
        Only called with a fresh valid connection
        """

        # get cursor
        cursor = self.conn.cursor()

        # logging
        logging.debug("MS: attempt to recover transaction")

        # perform all operations in current newly created transaction
        for sqlOperation in self.transaction:
            cursor.execute(sqlOperation)

        # logging
        logging.warning("MS: transaction recovered")

        # close cursor
        cursor.close()

    ##########################################################################
    # purgeMessages method 
    ##########################################################################

    def purgeMessages(self):
        """
        __purgeMessages__
        
        Drop all messages to be delivered. 
        """

        # logging
        logging.debug("MS: purgeMessages requested")

        # get cursor
        cursor = self.conn.cursor()

        # remove all messsages
        sqlCommand = """
                     DELETE 
                       FROM ms_message
                     """
        cursor.execute(sqlCommand)

        # drop transaction status, no recover possible
        self.transaction = []

        # commit
        self.conn.commit()

        # return 
        cursor.close()
        return 

    ##########################################################################
    # remove messages of a certain time addressed to me
    ##########################################################################

    def remove(self, messageType):
        """
        __remove__

        Remove all messages of a certain type addressed to me.
        """

        # logging
        logging.debug("MS: remove messages of type %s." % messageType)

        # get cursor
        cursor = self.conn.cursor()

        # get message type (if it is in database)
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + messageType + """'
                     """
        cursor.execute(sqlCommand)
        rows = cursor.rowcount

        # no rows, nothing to do
        if rows == 0:

            return

        # get type
        row = cursor.fetchone()
        typeid = row[0]

        # remove all messsages
        sqlCommand = """
                     DELETE
                       FROM ms_message
                      WHERE type='""" + str(typeid) + """'
                        AND dest='""" + str(self.procid) + """'
                     """
        cursor.execute(sqlCommand)

        # drop transaction status, no recover possible
        self.transaction = []

        # commit
        self.conn.commit()

        # return
        cursor.close()
        return

    ##########################################################################
    # remove messages in history
    ##########################################################################

    def cleanHistory(self, hours):
        """
        __cleanHistory__
        
        Delete history messages older than the number of hours
        specified.
        
        Performs an implicit commit operation.
        
        Arguments:
        
            hours -- the number of hours.
        
        """

        # logging
        logging.debug("MS: clean history requested")

        # get cursor
        cursor = self.conn.cursor()

        timeval = "-%s:00:00" % hours

        # remove all messsages
        sqlCommand = """
                     DELETE 
                       FROM ms_history
                       WHERE
                          time < ADDTIME(CURRENT_TIMESTAMP,'-%s');
                          
                          """ % timeval

        #"""
        #TIMESTAMPADD(HOUR,""" + \
        #str(-1 * hours) + \
        #       """,CURRENT_TIMESTAMP);
        #       """
        cursor.execute(sqlCommand)

        # drop transaction status, no recover possible
        self.transaction = []

        # commit
        self.conn.commit()

        # return 
        cursor.close()
        return 

    ##########################################################################
    # get a list from elements in a row
    ##########################################################################

    def getList(self, cursor):
        """
        __getList__
        
        Auxiliar function to convert a row returned from an SQL query into
        a standard list. 
        
        """
        list = []
        while True:
            row = cursor.fetchone()
            if row == None:
                break
            list.append(row[0])
        return list

    ##########################################################################
    # get an open connection to the database
    ##########################################################################
                                                                                
    def connect(self, invalidate = False): 
        """
        __connect__
                                                                                
        return a DB connection, reusing old one if still valid. Create a new
        one if requested so or if old one expired.
                                                                                
        """

        # is it necessary to refresh the connection?
        
        if (time.time() - self.connectionTime > self.refreshPeriod or invalidate):
            
            #  close current connection (if any)
            try:
                self.conn.close()
            except:
                pass
            
            # create a new one    
            conn = connect(False)
            self.connectionTime = time.time()
                
            # set transaction properties
            cursor = conn.cursor()
            cursor.execute(\
                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET AUTOCOMMIT=0")
            cursor.close()
            
            # logging
            logging.debug("MS: got connection to database")

            # return connection handler
            return conn
        
        # return old one
        return self.conn


    ##########################################################################
    # close connection to the database
    ##########################################################################

    def close(self):
        """
        __close__

        close the DB connection

        """

        try:
            self.conn.close()
        except:
            pass
 
 
    #############################################################################
    # execute given sql, reconnect to db if neccesary
    #############################################################################
 
    def executeSQLwithRetry(self, sqlCommand, cursor = None):
        """
            Helper function that:
                creates cursor (Optionally)
                execute SQL with error handling
                return cursor
        """
        
        try:
            if cursor is None:
                cursor = self.conn.cursor()
                
            cursor.execute(sqlCommand)
            
        except:
            
            # logging
            logging.warning("MS: connection to database lost")

            # if it does not work, we lost connection to database.
            self.conn = self.connect(invalidate = True)

            # logging
            logging.warning("MS: connection to database recovered")
                                                                                
            # redo operations in interrupted transaction
            self.redo()
            
            # get cursor
            cursor = self.conn.cursor()

            # retry
            cursor.execute(sqlCommand)
        
        return cursor
