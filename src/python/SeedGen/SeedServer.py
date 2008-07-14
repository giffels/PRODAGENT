#!/usr/bin/env python
"""
XMLRPC Server for using SeedGen from a remote host

"""
import os
import sys
import traceback
import SocketServer,BaseHTTPServer
import xmlrpclib
from SeedGen import getSeedGen
_SeedCreator = getSeedGen()



#  //----------------------------------------------------------
# // Frederic Lundh's xmlrpcserver included in this module
#//  for easy distribution
#
# XML-RPC SERVER
# $Id: SeedServer.py,v 1.1 2006/03/08 22:48:16 evansde Exp $
#
# a simple XML-RPC server for Python
#
# History:
# 1999-02-01 fl  created
# 2001-10-01 fl  added to xmlrpclib distribution
# 2002-06-27 fl  improved exception handling (from Peter Åstrand)
#
# written by Fredrik Lundh, January 1999.
#
# Copyright (c) 1999-2002 by Secret Labs AB.
# Copyright (c) 1999-2002 by Fredrik Lundh.
#
# fredrik@pythonware.com
# http://www.pythonware.com
#
# --------------------------------------------------------------------
# Permission to use, copy, modify, and distribute this software and
# its associated documentation for any purpose and without fee is
# hereby granted.  This software is provided as is.
# --------------------------------------------------------------------
#
#  //
# //
#//
class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            # get arguments
            data = self.rfile.read(int(self.headers["content-length"]))
            params, method = xmlrpclib.loads(data)

            # generate response
            try:
                response = self.call(method, params)
                # wrap response in a singleton tuple
                response = (response,)
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
        # override this method to implement RPC methods
        print "CALL", method, params
        return params
#  //
# //   End RequestHandler xmlrpcserver class
#//---------------------------------------------------------


#  //-------------------------------------------------------
# // XMLRPC Random Seed Server 
#//  Heavily based on Dave Warner's Excellent article at
#  //http://www.onlamp.com/pub/a/python/2001/01/17/xmlrpcserver.html
# //
#//
#  //
# // Dave Evans  2/7/3 
#//
class SeedRequestHandler(RequestHandler):
    def call(self, method, params):
        print "Dispatching: ", method, params
        try:
            server_method = getattr(self, method)
        except:
            raise AttributeError, "Server does not contain XML-RPC procedure %s" % method
        return server_method(method, params)

    def dump_methodcall(self, method, params):
        return xmlrpclib.dumps(params[1:], params[0])

    def dump_params(self, method, params):
        return xmlrpclib.dumps(params)

    def test(self, method, nr):
        return nr

    def dump_response(self, method, params):
        response = self.call(params[0], tuple(params[1:]))
        return xmlrpclib.dumps(response)

    #  //
    # // This method creates a random seed using the 
    #//  SeedGen module and returns it.
    def create_seed(self,method,params):
        return _SeedCreator.createSeed()
#  //
# //
#//-------------------------------------------------------------

#  //-----------------------------------------------------------
# // Run this module to start the server on localhost port 8080
#//
if __name__ == '__main__':
    server = SocketServer.TCPServer(('', 8080), SeedRequestHandler)
    server.serve_forever()

    

