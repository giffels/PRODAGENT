#!/usr/bin/env python

import os,string,commands
import exceptions
"""
_DLS_
 
Class to extract/insert info to DLS based on DLS CLI
  
"""
# ##############
class DLSConfigError(exceptions.Exception):
  def __init__(self,APIdir):
   args="The DLS API directrory %s do not exist \n"%APIdir
   exceptions.Exception.__init__(self, args)
   pass

  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

# ##############
class DLSError:
  def __init__(self, FileBlock):
    print '\nERROR accessing DLS for fileblock '+FileBlock+'\n'
    pass

# ##############
class DLSCLIError(exceptions.Exception):
  def __init__(self, cmd, ErrorMessage):
   args = "Failing command: %s \n Failing reason: %s" % (cmd,ErrorMessage)
   exceptions.Exception.__init__(self, args)
   pass
                                                                                                                               
  def getClassName(self):
    """ Return class name. """
    return "%s" % (self.__class__.__name__)
                                                                                                                               
  def getErrorMessage(self):
    """ Return exception error. """
    return "%s" % (self.args)

# ##############
class DLSNoReplicas(exceptions.Exception):
  def __init__(self, FileBlock):
    args ="No replicas exists for fileblock: "+FileBlock+"\n"
    exceptions.Exception.__init__(self, args)
    pass

  def getClassName(self):
    """ Return class name. """
    return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
    """ Return exception error. """
    return "%s" % (self.args)

# ##############
class DLS:
  """
  _DLS_
 
  interface to extract/insert info from/to DLS
  """
# ##############
  def __init__(self):
          self.DLSServer_ = 'cmslcgco01.cern.ch'
          #self.DLSServerPort_ = '18081'
          self.DLSServerPort_ = '18080'
          self.DLSclient_ = 'DLSAPI'

          ## add check on 'PRODAGENTLITE_CONFIG' being set
          installDir=os.path.dirname(os.environ['PRODAGENTLITE_CONFIG'])
          DLSDir = string.replace(installDir, 'install','src/python/DLSInterface')
          DLSConfig=os.path.join(DLSDir, self.DLSclient_ )
          if not os.path.exists(DLSConfig):
           raise DLSConfigError(DLSConfig)

          self.DLSclientdir_=DLSConfig

# ##############
  def getReplicas(self, FileBlock):
         """
          query DLS to get replicas
         """
         cmd = self.DLSclientdir_+"/dls-get-se --port "+self.DLSServerPort_+" --host "+self.DLSServer_+" --datablock "+FileBlock 
         out=commands.getstatusoutput(cmd)
                                                                                                                               
         if out[0]==0:
          if out[1]!="":
            ListLocations=string.split(string.strip(out[1]),'\n')
          else:
            raise DLSNoReplicas(FileBlock)
         else:
          raise DLSCLIError(cmd, out[1])

         return ListLocations

# ##############
  def addReplica(self, FileBlock, Location):
         """
          insert into DLS a replica  ( = fileblock - location mapping)
         """
#         cmd = self.DLSclientdir_+"/dls-add-replica --port "+self.DLSServerPort_+" --host "+self.DLSServer_+" --datablock "+FileBlock+" --se "+Location
         cmd = "echo "+self.DLSclientdir_+"/dls-add-replica --port "+self.DLSServerPort_+" --host "+self.DLSServer_+" --datablock "+FileBlock+" --se "+Location
         print cmd
         out=commands.getstatusoutput(cmd)
         
## DLS error code now (from 6 Jan 2006) replaced with:
# 0 : Replica Registered      \  OK
# 1 : Replica already stored  /
# 2 : Replica not registered  > Error
# 3 : Server not respond      > Error

## error from getstatusoutput out[0] = 256*(command exit code)
#          print "return status %i"%out[0]
         if out[0]>=2*256 : 
           raise DLSCLIError(cmd, out[1])
        

##################################################################
# Unit testing
                                                                                                                               
if __name__ == "__main__":

  try:
    dlsinfo=DLS()

   ## get replicas
    try:
     fileblock="fakefileblock"
     #fileblock="bt_Hit752_g133/bt03c_pion_pt1"
     #fileblock="hg_Hit752_g133/tt_ch_170_tb20"
     replicas=DLS().getReplicas(fileblock)
     for replica in replicas:
      print "replica is %s"%replica

    except DLSCLIError, ex:
     print "Caught exception %s: \n %s" % (ex.getClassName(), ex.getErrorMessage())
    except DLSNoReplicas, ex:
     print "Caught exception %s: \n %s " % (ex.getClassName(), ex.getErrorMessage())
    except Exception, ex:
     print "Caught exception %s: "%ex

   ## insert replicas
    try:
     fileblock="fakefileblock"
     location="fakeSE"
     DLS().addReplica(fileblock,location) 
    except DLSCLIError, ex:
     print "Caught exception %s: \n %s" % (ex.getClassName(), ex.getErrorMessage())


  except DLSConfigError, ex:
     print "Caught exception %s: \n %s "% (ex.getClassName(), ex.getErrorMessage())
