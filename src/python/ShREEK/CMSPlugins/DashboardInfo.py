#!/usr/bin/env python
"""
_DashboardInfo_

Serialisable dictionary that provides default fields for
submit time dashboard registration that can be saved in the job
and used to republish/refresh that information at runtime
as well.


"""

import os
import types

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from ShREEK.CMSPlugins.ApMonLite.ApMonDestMgr import ApMonDestMgr


class DashboardInfo(dict):
    """
    _DashboardInfo_

    Serialisable dictionary of dashboard parameters

    task and job attributes are the MonALISA /cluster/host
    settings.
    
    """
    
    def __init__(self):
        dict.__init__(self)
        self.task = None
        self.job = None
        self.destinations = {}
        self.publisher = None
        self.setdefault("Application", None)
        self.setdefault("ApplicationVersion", None)
        self.setdefault("GridJobID", None)
        self.setdefault("LocalBatchID", None)
        self.setdefault("GridUser", None)
        self.setdefault("User" , os.environ.get('USER', 'ProdAgent'))
        self.setdefault("JSTool","ProdAgent")
        self.setdefault("NbEvPerRun", 0)
        self.setdefault("NodeName", None)
        self.setdefault("Scheduler", None)
        self.setdefault("TaskType", "production")
        self.setdefault("NSteps", 0)
        self.setdefault("VO", "CMS")
        self.setdefault("TargetCE", None)
        self.setdefault("RBname", None)
        self.setdefault("JSToolUI" , "ProdAgent")

        
        


    def save(self):
        """
        _save_

        Convert self to IMProv Structure for saving

        """
        result = IMProvNode(self.__class__.__name__, None,
                            Task = self.task,
                            Job = self.job,
                            )

        typeMap = { types.IntType : "Int",
                    types.FloatType : "Float",
                    types.NoneType : "None",
                    }
                    
        for key, value in self.items():
            typeVal = typeMap.get(type(value), "String")
            
            result.addNode(
                IMProvNode("Parameter", str(value), Name = key,
                           Type = typeVal)
                )
        for key, value in self.destinations.items():
            result.addNode(IMProvNode("Destination",
                                      None, Host=key, Port=value))
            
        return result
    
    def load(self, improvNode):
        """
        _load_

        Unserialise IMProvNode into self

        """
        
        paramQ = IMProvQuery("%s/Parameter" % self.__class__.__name__)
        destQ = IMProvQuery("%s/Destination" % self.__class__.__name__)
        self.job = improvNode.attrs.get("Job", None)
        self.task = improvNode.attrs.get("Task", None)
        if self.job != None: self.job = str(self.job)
        if self.job != None: self.task = str(self.task)
        
        params = paramQ(improvNode)
        for param in params:
            typeAttr = str(param.attrs.get("Type"))
            nameAttr = str(param.attrs.get("Name"))
            value = str(param.chardata)
            if typeAttr == "Int":
                value = int(value)
            elif typeAttr == "Float":
                value = float(value)
            elif typeAttr == "None":
                value = None
            self[nameAttr] = value
        dests = destQ(improvNode)
        for dest in dests:
            host = str(dest.attrs.get("Host"))
            port = int(dest.attrs.get("Port"))
            self.addDestination(host, port)

        return

    
            
    def read(self, filename):
        """
        _read_

        Read contents of file and extract DashboardInfo from it and
        populate self with it

        """
        try:
            improvNode = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Unable to load DashboardInfo from file:\n"
            msg += filename
            msg += "\n%s" % str(ex)
            raise RuntimeError(msg)
        query = IMProvQuery(self.__class__.__name__)
        node = query(improvNode)[0]
        self.load(node)
        return

    def write(self, filename):
        """
        _write_

        Serialise self to file provided

        """
        doc = IMProvDoc("DashboardMonitoring")
        doc.addNode(self.save())
        handle = open(filename, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return
        
    def addDestination(self, host, port):
        """
        _addDestination_

        Add an ApMon Destination to be published to from this instance

        """
        if self.publisher == None:
            self._InitPublisher()
        self.destinations[host] = port
        self.publisher.newDestination(host, port)
        return
        
    def publish(self, redundancy = 1):
        """
        _publish_

        Publish information in this object to the Dashboard
        using the ApMon interface and the destinations stored in this
        instance.

        redunancy is the amount to times to publish this information

        """
        if self.publisher == None:
            self._InitPublisher()
      
        
        self.publisher.connect()
        toPublish = {}
        toPublish.update(self)
        for key, value in toPublish.items():
            if value == None:
                del toPublish[key]
        
        for i in range(1, redundancy+1):
            self.publisher.send(**toPublish)
            
        self.publisher.disconnect()
        return


    def emptyClone(self):
        """
        _emptyClone_

        Return a copy of self including only the task, job and destination
        information

        """
        result = DashboardInfo()
        result.task = self.task
        result.job = self.job = self.job
        result.destinations = self.destinations
        return result
        
    def _InitPublisher(self):
        """
        _InitPublisher_

        *private*
        
        Initialise the ApMonDestMgr instance, verifying that the task and
        job attributes are set

        """
        if self.task == None:
            msg = "Error: You must set the task id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        if self.job == None:
            msg = "Error: You must set the job id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        self.publisher = ApMonDestMgr(self.task, self.job)
        return
        

        
