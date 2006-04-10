#!/usr/bin/env python
"""
_XMLRPCMonitor_

Monitor Plugin to provide an XML-RPC Service
to query the monitor state via remote rpc calls
using XMLRPC.
"""


__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: XMLRPCMonitor.py,v 1.1 2005/12/30 18:54:28 evansde Exp $"
__author__ = "evansde@fnal.gov"


from ShREEK.ShREEKMonitor import ShREEKMonitor
from ShREEK.ShREEKPluginMgr import registerShREEKMonitor

import os
import xmlrpclib
import sys
import traceback
import threading
import socket
import SocketServer
from  BaseHTTPServer import BaseHTTPRequestHandler


class RegistrationClient:
    """
    _RegistrationClient_

    If registration server details are provided, use this client
    to connect to it and register the job.
    """
    def __init__(self, address):
        """
        Constructor taking service host url
        """
        self._server = xmlrpclib.Server(address)
        
    def registerJob(self, jobName, userDN, rpcUrl):
        return self._server.registerJob(jobName, userDN, rpcUrl)

    def unregisterJob(self, jobName, userDN):
        return self._server.unregisterJob(jobName, userDN)



class XMLRPCHandler(BaseHTTPRequestHandler):
    """
    HTTP Server Request Handler to provide a POST response to
    http requests that calls the appropriate methods to access
    the MonitorState.
    """
    
    stateReference = {}
    monitorReference = None
    
    def do_POST(self):
        """
        Do POST action.
        """
        try:
            # get arguments
            data = self.rfile.read(int(self.headers["content-length"]))
            params, method = xmlrpclib.loads(data)
            
            # generate response
            try:
                response = self.call(method, params)
                # wrap response in a singleton tuple
                response = (response, )
            except:
                # print exception to stderr (to aid debugging)
                traceback.print_exc(file=sys.stderr)
                # report exception back to server
                response = xmlrpclib.dumps(
                    xmlrpclib.Fault(1, "%s:%s" % sys.exc_info()[:2])
                    )
            else:
                response = xmlrpclib.dumps(
                    response, 
                    methodresponse=1
                    )
        except:
            # internal error, report as HTTP server error
            traceback.print_exc(file=sys.stderr)
            self.send_response(500)
            self.end_headers()
        else:
            # got a valid XML RPC response
            self.send_response(200)
            self.send_header("Content-type", "text/xml")
            self.send_header("Content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

            # shut down the connection (from Skip Montanaro)
            self.wfile.flush()
            self.connection.shutdown(1)


    def call(self, method, params):
        """
        _call_

        Method to provide access to the MonitorState parameters
        """
        try:
            serverMethod = getattr(self, method)
        except:
            msg = "Server does not contain XML-RPC procedure %s" % method
            raise AttributeError, msg
        return serverMethod(method, params)


    def getMetric(self, method, params):
        """
        _getMetric_

        This is the actual web service method that will be called,
        it will interpret params[0] as a key in the MonitorState
        Mapping and return the value associated with that key.
        """
        try:
            result = self.stateReference.get(
                params[0], "Invalid Key: %s" % params[0])
        except IndexError:
            result = xmlrpclib.Fault(1, "Not enough Arguments supplied:" % \
                                     (sys.exc_info()[0],
                                      sys.exc_info()[2], key))
        return result
    

    def killswitch(self, method, params):
        """
        _killswitch_

        Tell ShREEK to kill the current process and exit
        """
        try:
            self.monitorReference.killJob()
            result = 0
        except StandardError, ex:
            result = xmlrpclib.Fault(1, "Error setting killswitch: %s" % ex)
        return result
    

    def jobComplete(self, method, params):
        """
        _jobComplete_

        Tell ShREEK to finish the current task normally an then
        exit running no further tasks
        """
        try:
            self.monitorReference.jobComplete()
            result = 0
        except StandardError, ex:
            result = xmlrpclib.Fault(1, "Error setting jobComplete: %s" % ex)
        return result
    

class ReuseSocketServer(SocketServer.TCPServer):
    """
    override SocketServer server_bind method to use
    Reuse address flag to prevent locking port for
    a short period after shutdown
    """
    def server_bind(self):
        """
        override base class to use REUSEADDR option
        """
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        SocketServer.TCPServer.server_bind(self)

class ServerThread(threading.Thread):
    """
    _ServerThread_

    Thread to run XMLRPC Monitoring Service in the background
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self.host = socket.gethostname()
        self.address = None
        self.startingPort = 8082
        self.endingPort = 8182
        for port in range(self.startingPort, self.endingPort):
            try:
                self._Server = ReuseSocketServer(
                    (self.host, port),
                    XMLRPCHandler)
                self.address = "http://%s:%s" % (self.host, port)
                break
            except socket.error:
                continue
        
    def run(self):
        """
        define what happens when the thread starts
        """
        self._Server.serve_forever()

        

        

class XMLRPCMonitor(ShREEKMonitor):
    """
    _XMLRPCMonitor_

    Start an XMLRPC http server on a port specified in the
    initialisation args to allow the MonitorState information
    to be retrieved by remote web service calls.
    """
    def __init__(self):
        ShREEKMonitor.__init__(self)
        self._Opts = {}
        self._StateRef = {}
        self._Server = None
        self._ServerURL = None
        self._Opts.setdefault('RegisterService', None)
        self._Opts.setdefault('UserDN', os.environ.get('USER', "nobody"))
        
        
    def initMonitor(self, *args, **kwargs):
        """
        _initMonitor_
        Initialize monitor by creating the XMLRPC Server
        in a daemon thread.
        """
        self._Opts.update(kwargs)
        XMLRPCHandler.monitorReference = self
        self._Server = ServerThread()
        self._Server.setDaemon(1)
        self._Server.start()
        msg =  "XMLRPCMonitor Started on %s" % self._Server.address 
        self._ServerURL = self._Server.address
        print msg
        
            
    def jobStart(self):
        """
        _jobStart_

        Override jobStart handler to register job with Registration
        Service if provided

        """
        if self._Opts['RegisterService'] != None:
            try:
                regClient = RegistrationClient(self._Opts['RegisterService'])
                regClient.registerJob(self.jobId, self._Opts['UserDN'],
                                      self._ServerURL)
            except socket.error, ex:
                msg = "Unable to register job with:"
                msg += "%s\n" % self._Opts['RegisterService']
                msg += str(ex)
                print msg
            
        return
    
    def jobEnd(self):
        """
        _jobEnd_

        Override jobEnd handler to unregister job if required
        """
        if self._Opts['RegisterService'] != None:
            try:
                regClient = RegistrationClient(self._Opts['RegisterService'])
                regClient.unregisterJob(self.jobId, self._Opts['UserDN'])
            except socket.error, ex:
                msg = "Unable to unregister job with:"
                msg += "%s\n" % self._Opts['RegisterService']
                msg += str(ex)
                print msg
        return
    
        
    def periodicUpdate(self, monitorState):
        """
        Update the state information in the
        RPC Handler
        """
        XMLRPCHandler.stateReference.update(monitorState)
        return
        
    def shutdown(self):
        """
        _shutdown_

        Shutdown the RPCServer
        """
        print "Shutting down XMLRPCMonitor Server..."
        del self._Server
        return

registerShREEKMonitor(XMLRPCMonitor, 'xmlrpc')
