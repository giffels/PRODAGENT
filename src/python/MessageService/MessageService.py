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

__revision__ = "$Id: MessageService.py,v 1.2 2006/05/03 08:14:28 ckavka Exp $"
__version__ = "$Revision: 1.2 $"
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
        self.maxConnectionAttemps = 5
        self.dbWaitingTime = 10
        self.pollTime = 5

        # open connection
        self.conn = self.connect()
        
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

    def publish(self, name, payload):
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

        # get cursor
        cursor = self.conn.cursor()

        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        try:
            cursor.execute(sqlCommand)
        except:

            # logging
            logging.warning("MS: connection to database lost")

            # if it does not work, we lost connection to database.
            self.conn = self.connect()

            # logging
            logging.warning("MS: connection to database recovered")
                                                                                
            # redo operations in interrupted transaction
            self.redo()
            
            # get cursor
            cursor = self.conn.cursor()

            # retry
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
                             (type, source, dest, payload)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            sqlCommand = """
                         INSERT
                           INTO ms_history
                             (type, source, dest, payload)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """')
                         """
            cursor.execute(sqlCommand)
            self.transaction.append(sqlCommand)
            destCount += 1

        # return
        cursor.close()
        return destCount
        
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

        # get cursor
        cursor = self.conn.cursor()

        # get messages command
        sqlCommand = """
                     SELECT messageid, name, payload
                       FROM ms_message, ms_type
                       WHERE
                         typeid=type and
                         dest='""" + str(self.procid) + """'
                       ORDER BY time,messageid
                       LIMIT 1
                       
                     """

        # check for messages
        while True:

            # get messsages
            try:
                cursor.execute(sqlCommand)
            except:

                # logging
                logging.warning("MS: connection to database lost")

                # if it does not work, we lost connection to database.
                self.conn = self.connect()
                                                                                
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
            self.conn = self.connect()

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
                                                                                
    def connect(self): 
        """
        __connect__
                                                                                
        Refresh the connection to the database, re-attempting a maximum of
        maxConnection attempts, waiting for dbWaitingTime between attempts.
                                                                                
        """

        # get a connection
        for attempt in range(self.maxConnectionAttemps):

            try:
                #  try to get one
                conn = connect()#dbName = "ProdAgentDB",\
                                #host = "cmslcgco01.cern.ch",\
                                #user = "Proddie",\
                                #passwd = "ProddiePass",\
                                #socketFileLocation = "",\
                                #dbPortNr = "")

                # set transaction properties
                cursor = conn.cursor()
                cursor.execute(\
                     "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cursor.execute("SET AUTOCOMMIT=0")
                cursor.close()
                break

            except:
                # logging
                logging.warning("MS: cannot connect to database, waiting")

                # failed, wait before trying again
                time.sleep(self.dbWaitingTime)
                conn = None

        # failed, abort
        if conn == None:

            # logging
            logging.error("MS: cannot connec to database, aborting")

            # abort
            raise

        # logging
        logging.debug("MS: got connection to database")

        # return connection handler
        return conn


