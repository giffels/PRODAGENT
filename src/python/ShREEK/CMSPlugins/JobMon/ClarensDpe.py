#     Clarens client using the Python HTTP transport
#     Supports GSI and SSL Authentication
#
#    Copyright (C) 2004 California Institute of Technology
#    Author: Conrad D. Steenberg <conrad@hep.caltech.edu>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or   
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of 
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the  
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.

# $Id: ClarensDpe.py,v 1.1 2006/03/16 18:38:20 evansde Exp $

import xmlrpclib, httplib
import cStringIO as StringIO

import os
import sys
import time
import string, urlparse
from base64 import encodestring, decodestring
import random
import gzip
import sha
import Cookie
import re
random.seed()
import getpass

client_registry={}
debug=1

#-------------------------------------------------------------------------------
def err_msg(mes):
  """Prints an error message on stderr
     Input: a string
   
     Returns: None
  """
  sys.stderr.write(mes+"\n")
  sys.stderr.flush()

#-------------------------------------------------------------------------------
def repr_headers(hdict):
  sio=StringIO.StringIO()
  for key, value in hdict.items():
    sio.write("%s: %s\n"%(key,value))
  
  return sio.getvalue()

#-------------------------------------------------------------------------------
def parse_response(f, conn, debug=0):
    """parse_response(file_obj)

    Read response from input file object, and parse it
    
    The parsed data is returned to the caller
    """
    p,u=xmlrpclib.getparser()
    if debug:
      s=StringIO.StringIO()
    i=0
    try:
      while 1:
        try:
#          print f.fp._line_consumed, f.fp._line_left
          response = f.read()
        except AssertionError:
          break
        if not response:
            break
        if debug:
          s.write(response)
        p.feed(response)
      if debug:
        err_msg(s.getvalue())
      p.close()
      return u.close()
    except:
      if debug:
        err_msg(s.getvalue())
      raise

#-------------------------------------------------------------------------------
def load_cert(certfile, debug=None):
  for fname in [certfile,'/tmp/x509up_u%s'%(os.getuid()),
                '%s/.globus/usercert.pem'%(os.environ['HOME'])]:
    if not fname: continue
    text_file=None
    try:
      text_file=open(fname)
    except:
      pass        
    if text_file: 
      certfile=fname
      break
  if not text_file:
    raise ValueError("Could not open certificate file. Tried:\n%s\n%s\n%s"\
      %(certfile,'/tmp/x509up_u%s'%(os.getuid()),
                '%s/.globus/usercert.pem'%(os.environ['HOME'])))
  if debug: err_msg("Using certificate from '%s'\n"%fname)
          
  text_ucert=text_file.read()
  text_file.close()
  return text_ucert, certfile

#-------------------------------------------------------------------------------
def load_key(keyfile, debug=None):
  for fname in [keyfile,'/tmp/x509up_u%s'%(os.getuid()),
                '%s/.globus/userkey.pem'%(os.environ['HOME'])]:
    if not fname: continue
    text_file=None
    try:
      text_file=open(fname)
    except:
      pass
    if text_file: 
      keyfile=fname
      break
  if not text_file:
    raise ValueError("Could not open key file. Tried:\n%s\n%s\n%s"\
      %(keyfile,'/tmp/x509up_u%s'%(os.getuid()),
                '%s/.globus/userkey.pem'%(os.environ['HOME'])))
  if debug: err_msg("Using key from '%s'\n"%fname)
          
  text_key=text_file.read()
  text_file.close()
  return text_key, keyfile


#-------------------------------------------------------------------------------
def create_session_key():
  """Creates a printable random set of characters
  
     Input:  no arguments
     Output: a string
  """
  ustring_raw="%s_%f_%f"%(os.getpid(),time.time(),random.random())
  ustring=sha.new(ustring_raw).hexdigest()
  return ustring

#-------------------------------------------------------------------------------
class _caller:
  '''Internal class used to give the user a callable object
    that calls back to the Binding object to make an RPC call.'''

  def __init__(self, binding, name):
    self.__binding, self.__name = binding, name

  def __getattr__(self, name):
    return _caller(self.__binding, "%s.%s" % (self.__name, name))
        
  def __call__(self, *args):
    return self.__binding.execute(self.__name, args)


#-------------------------------------------------------------------------------
class client:
  def __init__(self, url, certfile=None, keyfile=None,
               callback=getpass.getpass, async=None,
               passwd=None, debug=0, cert_text=None, key_text=None,
               progress_cb=None):

      global client_registry
      self.pwcallback=callback
      self.passwd_set(passwd)
      self.debug=debug
      self.login_callback=None
      self.url=url
      self.callback=None
      self.callback_args=None
      self.file_obj=None
      self.server_cert=None
      self.deserialize=1
      self.filename=None

      self.url=url

      ustring=create_session_key()
      
      # Parse URL to see if we need to load key/cert ourselves
      purl=urlparse.urlparse(url)
      hostport=string.split(purl[1],":")
      if len(hostport)==1:
        self.host=hostport[0]
        self.port=443
      elif len(hostport)==2:
        self.host=hostport[0]
        self.port=int(hostport[1])

      if debug:
        err_msg("(host,port,url)=(%s,%s,%s)"%(self.host,self.port,self.url))

      
      # Read certificate file, or use supplied text
      if string.lower(purl[0])!='https':
        err_msg("This client only supports HTTPS (SSL) connections")
        return
      else:
        text_ucert, self.certfile=load_cert(certfile)
        text_ukey, self.keyfile=load_key(keyfile)
        self.conn_passwd='BROWSER'

      # Create transport object
      self.conn=httplib.HTTPSConnection(self.host, self.port, self.keyfile, self.certfile)

      self.debug=debug
      self.proto=string.lower(purl[0])

      self.conn_user=ustring
      login_num=0

      if not async:
        try:
          self.do_login(text_ucert)
          client_registry[str(id(self))]=debug
        except:
          raise
      else:
        self.perform_callback=self.login_handler

  #-----------------------------------------------------------------------------
  def __getattr__(self, name):
    '''Return a callable object that will invoke the RPC method
     named by the attribute.'''
    if name[:2] == '__' and len(name) > 5 and name[-2:] == '__':
            if self.__dict__.has_key(name): 
              return self._dict__[name]
            return self.__class__.__dict__[name]
    return _caller(self, name)

  #-----------------------------------------------------------------------------
  def re_use(self,url,username,passwd):
    self.url=url
    self.conn_user=username
    self.conn_passwd=passwd

  #-----------------------------------------------------------------------------
  def nb_dispatch(self,method,args,timeout=5):
    request_body=xmlrpclib.dumps(tuple(args),method)


    self.xmlrpc_h={"Content-Type": "text/xml",
                   "User-Agent": "Clarens_curl_async.py version 1.4"
                  }
    xmlrpc_h={"Content-Type": "text/xml",
              "User-Agent": "Clarens_curl_async.py version 1.4",
              "AUTHORIZATION" : "Basic %s" % string.replace(
               encodestring("%s:%s" % (self.conn_user, self.conn_passwd)),"\012", "")
             }
    connected=0
    count=0
    while not connected and count<4:
      try:
        if self.debug:
          err_msg("sending request...")
          err_msg(repr_headers(xmlrpc_h))
          err_msg(request_body)
        self.conn.request("POST","/clarens/",request_body,xmlrpc_h)
        connected=1
      except httplib.CannotSendRequest,v:
        err_msg(repr(v))
        connected=0
      except Exception,v:
        if self.debug:
          err_msg("state = %s"%self.conn._HTTPConnection__state)
        connected=0
      count=count+1

  #-----------------------------------------------------------------------------
  def get_result(self):
    return self.conn.getresponse()
  
  #-----------------------------------------------------------------------------
  def execute(self,method,args):
    i=0
    self.nb_dispatch(method,args)
    stime=0.1
    while i<7:
      try:
        self.result_obj=self.get_result()
      except httplib.ResponseNotReady:
        pass
      except httplib.BadStatusLine:
        self.conn=httplib.HTTPSConnection(self.host, self.port, self.keyfile, self.certfile)
        self.nb_dispatch(method,args)        
      if self.result_obj.length==None:
#        err_msg("self.result_obj.length==None")
        break
      i=i+1
      
    # Return data in file or as string to user
    if not self.deserialize:
      if not self.filename:
        s=StringIO.StringIO()
      else:
        s=open(filename,"w")
      try:
        while 1:
          data = self.result_obj.read()
          if not data:
              break
          s.write(data)
      except:
        raise
      if not self.filename:
        return s.getvalue()
      else:
        return s
    # Return parsed response
    retval = parse_response(self.result_obj, self.conn, self.debug)
    return retval[0]

  #-----------------------------------------------------------------------------
  def do_login(self, passwd):
    if self.proto=='http':
      err_msg("This client only supports HTTPS (SSL) connections")
    elif self.proto=='https':
      try:
        response=self.execute("system.auth2",[])
      except:
        raise
      self.https_login_handler(response)

  #-----------------------------------------------------------------------------
  def login_handler(self, values, extra=None):
    if self.proto=='http':
      err_msg("This client only supports HTTPS (SSL) connections")
    elif self.proto=='https':
      return self.https_login_handler(values,extra)

  #-----------------------------------------------------------------------------
  def https_login_handler(self, values, extra=None):
    server_cert, user_cert, new_passwd=values
    self.conn_passwd=new_passwd
    client_registry[str(id(self))]=debug

  #-----------------------------------------------------------------------------
  def error_handler(self, source, exc):
    if hasattr(self,"err_callback"):
      return self.err_callback(self, source, exc, self.err_callback_args)
    else:
      return None

  #-----------------------------------------------------------------------------
  def set_error_callback(self, func, args):
    if self.client:
      self.client.setOnErr(self.error_handler)
    self.err_callback=func
    self.err_callback_args=args

  #-----------------------------------------------------------------------------
  def set_writefunction(self,cb):
      self.writefunction=cb
      self.filename=None

  #-----------------------------------------------------------------------------
  def set_file_download(self,filename):
    self.deserialize=0
    self.filename=filename
    fname=os.path.split(filename)[1]
    self.writefunction=None
    if not self.filename: 
      raise IOError("%s is a directory!"%filename)

  def disable_deserialize(self):
    self.deserialize=0
    
  def enable_deserialize(self):
    self.deserialize=1
    self.file_obj = StringIO.StringIO()
    self.writefunction=None
    self.filename=None

  #-----------------------------------------------------------------------------
  def file_progress(self, dltotal, dlnow, ultotal, ulnow):
      k=1024
      M=k*1024
      G=M*1024
      
      post='bytes'
      v=dlnow
      vt=dltotal
      if dltotal>=k and dlnow<M:
        v=dlnow/k
        vt=dltotal/k
        post='k'
      elif dlnow>=M and dlnow<G:
        v=dlnow/M
        vt=dltotal/M
        post='M'
      elif dlnow>=G:
        v=dlnow/G
        vt=dltotal/M
        post='G'
      sys.stdout.write("\r %2.1f %s"%(v,post))
      sys.stdout.flush()

  #-----------------------------------------------------------------------------
  def set_progress_callback(self,cb):
      self.progress_cb=cb

  #-----------------------------------------------------------------------------
  def passwd(self,v):
      if self.priv_passwd==None:
        self.priv_passwd=self.pwcallback(v)
      return self.priv_passwd

  #-----------------------------------------------------------------------------
  def passwd_set(self,passwd=None):
      self.priv_passwd=passwd

  #-----------------------------------------------------------------------------
  def get_url(self):
    return self.url

  #-----------------------------------------------------------------------------
  def get_session_id(self):
    return self.conn_user,self.conn_passwd()

  #-----------------------------------------------------------------------------
  def __del__(self):
    if self.debug:
      err_msg("Logging out")
      
    obj_id=str(id(self))
    if not client_registry.has_key(obj_id): return
    debug=client_registry[obj_id]
    try:
      logout=self.execute("system.logout",[])
      if self.debug:
        err_msg("Logout returned %s\n"%logout)
    except:
      if debug:
        raise
      else:
        pass

    del client_registry[obj_id]

  #-----------------------------------------------------------------------------
  def test(self,reps=1000):
    """Test the response speed of a server, printing the time a certain number of calls
       took to execute

       Input: number of repetitions (optional)
       
       Output: None
       """
    starttime=time.time()
    for i in xrange(reps):
      self.execute("echo.echo",["Hello"])
    endtime=time.time()

    print "Elapsed time is ",endtime-starttime,"s, ",reps/(endtime-starttime),\
      " calls/s for ",reps," calls"
