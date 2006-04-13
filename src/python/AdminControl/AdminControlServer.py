#!/usr/bin/env python
"""
_AdminControlServer_

XMLRPC Server object in which an AdminControlInterface is instantiated
and used to provide external access to the components

"""
import socket
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
from SocketServer import ThreadingTCPServer

from AdminControl.AdminControlInterface import AdminControlInterface


class AdminControlServer(ThreadingTCPServer, SimpleXMLRPCDispatcher):
    """
    _AdminControlServer_

    Basic Threaded version of SimpleXMLRPCServer with socket reuse enabled
    to allow easy creation of clients.

    Overrides the server_bind method to allow socket reuse to avoid tying
    up TCP ports until they time out
    """
    def __init__(self, clientHost, clientPort):
        SimpleXMLRPCDispatcher.__init__(self)
        ThreadingTCPServer.__init__(self,
                                    (clientHost, clientPort),
                                    SimpleXMLRPCRequestHandler)
        self.logRequests = 1
        self.client = AdminControlInterface()
        self.register_introspection_functions()
        self.register_instance(self.client)
        

    
    def server_bind(self):
        """
        override base class to use REUSEADDR option
        """
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        ThreadingTCPServer.server_bind(self)

