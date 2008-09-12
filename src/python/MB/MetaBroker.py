#!/usr/bin/env python
# pylint: disable-msg=W0613,W0152
# disable bad ** magic and unused args warnings
"""
_MetaBroker_

Common Base Class for MetaBroker file-like objects.
Provides dictionary structure and path manipulation,
as well as transport and query mechanism

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: MetaBroker.py,v 1.1 2005/12/30 18:51:37 evansde Exp $"

import os
import socket
import re

from MB.MBException import MBException

class MetaBroker(dict):
    """
    _MetaBroker_

    Abstract MetaBroker class that provides dictionary
    interface to manipulate file like objects.

    Provides a set of default fields and handlers for those fields.

    Fields: --
    
    - *BaseName* :  Name of the object (no path) 

    - *PathName* :  Path to file, can contain vars, be relative etc.

    - *HostName* :  Name of node on which object resides

    - *AbsName* :     Absolute filename with variables expanded

    - *RemoteName* :  Combination of HostName:AbsName 

    - *Filesize* :    Size of file

    - *TransportMethod* :  File transport method, eg cp,rcp, scp etc

    - *URL* : Output field for Transport. After a Transport operation
    the transporter will set this to reflect the new location of the file
    
    - *QueryMethod* :  Tag for performing existence tests
    
    - *TargetBaseName* :   Target base name field

    - *TargetPathName* :   Target path name field

    - *TargetHostName* :   Target host name field

    - *TargetAbsName* : Absolute expanded target pathname

    - *TargetRemoteName* :   Complete target path using Target file,path, host

    - *Target* : Target representation, if remote, then this will be
    the TargetRemoteName if not it will be the TargetAbsName.
    Target should not be set directly.

    - *SourceBaseName* : BaseName of Source Object

    - *SourcePathName* : PathName of Source Object (including varnames etc)

    - *SourceAbsName* : Abs Name of Source Object (no variables)

    - *SourceHostName* : Host Name of Source Object

    - *SourceRemoteName* : Remote Name of Source Object hostname:abspath

    - *Source* : Source Representation, if remote source then this will
    be the SourceRemoteName, if local, SourceAbsName. Source Should not
    be set directly
    
    """
    _MetaBrokerInitFields = [
        "BaseName",         
        "PathName",         
        "HostName",         
        "AbsName",          
        "RemoteName",
        "URL",
        "Filesize",         
        "TransportMethod",
        "TransportIn",
        "TransportOut",
        "QueryMethod",
        "CreatorMethod",
        "TargetBaseName",   
        "TargetPathName",   
        "TargetHostName",   
        "TargetAbsName",
        "TargetRemoteName",
        "Target",
        "SourceBaseName",   
        "SourcePathName",   
        "SourceHostName",   
        "SourceAbsName",
        "SourceRemoteName",
        "Source",


        ]
    
    def __init__(self, **args):
        """
        Constructor sets default fields.
        Set methods are provided to init the dictionary entries
        for the default fields. 
        Other fields can be added dynamically using the usual 
        dictionary interfaces.

        Args --

        - *args* : Set of Keyword=Value pairs to initialise
        the object with
        """
        dict.__init__(self)
        #  //
        # // Create Default fields
        #//
        for item in self._MetaBrokerInitFields:
            self.setdefault(item, None)
        #  //
        # // Children associated with this object.
        #//  Children are also represented by MetaBroker objects
        #  //and are transported with parents
        # //
        #//
        self._Children = {}
        self._Parent = None
        #  //
        # // SyncKeys is a list of keys that must be kept
        #//  in sync with a parent metabroker, that is,
        #  //if TransportMethod is in this 
        # // list, when this meta broker's parent (if any)
        #//  changes values, then it will make sure that
        #  //this MB will have the same value for 
        # // TransportMethod
        #//
        self._SyncKeys = []
        #  //
        # // Observers on this MetaBroker.
        #//
        self._Observers = []
        
        #  //
        # // Setup Set handler methods for default 
        #//  fields
        self._SetHandlers = {
            'BaseName' : self._MBSetBaseName ,
            'PathName' : self._MBSetPathName,
            'HostName' : self._MBSetHostName,
            'AbsName'  : self._MBSetAbsName,
            'RemoteName' : self._MBSetRemoteName,
            'TargetBaseName' : self._MBSetTargetBaseName,
            'TargetPathName' : self._MBSetTargetPathName,
            'TargetAbsName'  : self._MBSetTargetAbsName,
            'TargetRemoteName' : self._MBSetTargetRemoteName,
            'SourceHostName' : self._MBSetSourceHostName,
            'SourceBaseName' : self._MBSetSourceBaseName,
            'SourcePathName' : self._MBSetSourcePathName,
            'SourceAbsName'  : self._MBSetSourceAbsName,
            'SourceRemoteName' : self._MBSetSourceRemoteName,
            'SourceHostName' : self._MBSetSourceHostName,

            }
        #  //
        # // Get Handler methods, used when getitem is used
        #//  to access a particular key
        self._GetHandlers = {
            'Filesize': self._MBGetFilesize,
            'HostName': self._MBGetHostName,
            'RemoteName':self._MBGetRemoteName,
            'AbsName':self._MBGetAbsName,
            'TargetRemoteName':self._MBGetTargetRemoteName,
            'TargetAbsName':self._MBGetTargetAbsName,
            'Target':self._MBGetTarget,
            'SourceRemoteName':self._MBGetSourceRemoteName,
            'SourceAbsName':self._MBGetSourceAbsName,
            'Source':self._MBGetSource,
            }



        #  //
        # // Process Constructor Args
        #//
        for key, val in args.items():
            self[key] = val
            
            
            

            
        #  //
        # //  End Ctor
        #//====================================


    def syncSource(self):
        """
        synchronise the basic values with the Source Values
        Ie: Set BaseName = SourceBaseName, PathName = SourcePathName
        """
        #  //
        # // Only need to sync these three as the others are 
        #//  generated from them
        self['BaseName'] = self['SourceBaseName']
        self['PathName'] = self['SourcePathName']
        self['HostName'] = self['SourceHostName']
        return

    def syncTarget(self):
        """
        synchronise the Target values with the basic values
        Ie: set TargetBaseName = BaseName, TargetPathName = PathName
        """
        self['TargetBaseName'] = self['BaseName']
        self['TargetPathName'] = self['PathName']
        self['TargetHostName'] = self['HostName']
        return
        


    def addChildMetaBroker(self, childName, mbInstance):
        """
        _addChildMetaBroker_

        Add a metabroker instance to this MetaBroker as a logical
        child of this object. During transport, any Child MetaBrokers
        are moved along with the parent. Child Objects are
        stored in a map of child name to MetaBroker instance

        - *childName* : Name of child in child map

        - *mbInstance* : MetaBroker instance relating to child

        """
        if not isinstance(mbInstance, MetaBroker):
            msg = "Non MetaBroker Object added as Child"
            raise MBException(
                msg, ModuleName = "MetaBroker.MetaBroker",
                ClassName = "MetaBroker",
                MethodName = "addChildMetaBroker",
                BadObject = mbInstance,
                )
        self._Children[childName] = mbInstance
        mbInstance.setParent(self)
        return

    def childNames(self):
        """
        _childNames_

        Accessor to list of names of child MetaBroker Objects
        stored in this object

        Returns --

        - *list* : List of names of children objects
        
        """
        return self._Children.keys()

    def children(self):
        """
        _children_

        Return list of child MetaBroker instances

        Returns --

        - *list* : List of MetaBroker child instances

        """
        return self._Children.values()

    def childrenMap(self):
        """
        _childrenMap_

        Return the _children dictionary

        Returns --

        - *dict* : Dictionary of child name:MetaBroker instances

        """
        return self._Children


    def setParent(self, mbInstance):
        """
        Set Reference to parent MetaBroker object.
        Should be called via addChildMetaBroker interface

        Args --

        - *mbInstance* : Parent MetaBroker Instance 
        """
        if not isinstance(mbInstance, MetaBroker):
            msg = "Non MetaBroker Object set as Parent"
            raise MBException(
                msg, ModuleName = "MetaBroker.MetaBroker",
                ClassName = "MetaBroker",
                MethodName = "setParent",
                BadObject = mbInstance,
                )
        
        self._Parent = mbInstance
        return


    def parent(self):
        """
        _parent_

        Return Parent reference, or None if no parent

        """
        return self._Parent

    def addSyncKey(self, key):
        """
        Add a SyncKey to this MetaBroker. When this
        MetaBroker is added to another MetaBroker as a child
        then keys listed in the Sync Keys will be updated against
        the parent values. 
        
        """
        if key not in self._SyncKeys:
            self._SyncKeys.append(key)
        return

    def syncKeys(self):
        """
        _syncKeys_

        return the list of SyncKeys for this object
        """
        return self._SyncKeys


    def execute(self, operator, *args, **keywords):
        """
        execute an operator 
        execute can be used to execute an operator on
        the MetaBroker and all of its children.The operator
        must be callable: Ie define the __call__ method for
        a class or be a function or method.
        The calling footprint should be:
        function(metabroker,*args,**keywords)
        or
        __call__(self, metabroker,*args,**keywords)
        
        Args: --
        
        - *operator* : Operator to be executed on this MetaBroker
        instance and then recursively on its children. Must be a
        callable oject.
 
        - *args* : Optional args to be used by the execute operator
        these are forwarded top the operator call
 
        - *keywords* : Optional keyword args to be used by the execute operator
        these are forwarded top the operator call
 
        Returns: --
 
        - *None* : Operators should store any output themselves.
         
        """
        if not callable(operator):
            msg = "Non callable operator passed to execute method"
            raise MBException(
                msg,  ModuleName = "MetaBroker.MetaBroker",
                ClassName = "MetaBroker",
                MethodName = "execute",
                Operator = operator)
        #  //
        # // Call operator on this object
        #//
        operator.__call__(self, *args, **keywords)
        for child in self._Children.values():
            #  //
            # // Call operator on each child
            #//
            child.execute(operator, *args, **keywords)
        return


    def topExecute(self, operator, *args, **keywords):
        """
        Call Execute from the topmost directory structure,
        this guarantees that every MetaBroker in the structure
        will be operated on. If a MetaBroker instance has a parent,
        then it passes the call up to its parent. If not, then
        the Execute method is called. The arguments must satisfy the
        same criteria as for the Execute method.

        Args: --
        
        - *operator* : Operator to be executed on this MetaBroker
        instance and then recursively on its children. Must be a
        callable oject.
 
        - *args* : Optional args to be used by the execute operator
        these are forwarded top the operator call
 
        - *keywords* : Optional keyword args to be used by the execute operator
        these are forwarded top the operator call
 
        Returns: --
 
        - *None* : Operators should store any output themselves.
         
        """
        if self._Parent == None:
            #  // 
            # // No Parent => Execute the operator on self
            #// 
            self.execute(operator, *args, **keywords)
        else:
            #  //
            # // Parent => refer operator to parent
            #//
            self._Parent.topExecute(operator, *args, **keywords)
        return


    def registerObserver(self, observer):
        """
        Register a MetaBrokerObserver instance with this object.
        The object must be an instance of MetaBrokerObserver.

        Args --

        - *observer* : Observer instance that will observe
        this instance
        
        """
        observerClass = observer.__class__.__name__
        observerBases = [observerClass]
        for item in  observer.__class__.__bases__:
            observerBases.append(item.__name__)
            
        if "MetaBrokerObserver" not in observerBases:
            msg = "Non MetaBrokerObserver Object added as Observer"
            raise MBException(
                msg, ModuleName = "MetaBroker.MetaBroker",
                ClassName = "MetaBroker",
                MethodName = "registerObserver",
                BadObject = observer,
                )
        if observer not in self._Observers:
            self._Observers.append(observer)
        return
    

    def update(self):
        """
        _update_

        Resynchronise values with any child metabroker objects,
        do this by calling the childs Sychronise method with this
        instance as an argument
        """
        for child in self._Children.values():
            child.synchronise(self)

        for observer in self._Observers:
            observer.updateObserver()
        return
            

    def synchronise(self, parent):
        """
        _synchronise_

        Set all SyncKeys to be the same value as that of the parent
        if any. This method is called by the parent MetaBroker Object
        """
        for item in self._SyncKeys:
            if parent.has_key(item):
                self[item] = parent[item]
        return


    def isSourceRemote(self):
        """
        _isSourceRemote_

        Test if Source values are for a remote node

        Returns --

        - *Bool* : True if Source is on a remote host, false if on local
        host

        """
        thisHost = socket.gethostbyaddr(socket.gethostname())[0]
        return not self['TargetHostName'] == thisHost


    def isRemote(self):
        """
        _isRemote_

        Test if the memory values are for this host or a remote host

        Returns --

        - *Bool* : True if Metabroker values point to a remote host,
        false if on local host

        """
        thisHost = socket.gethostbyaddr(socket.gethostname())[0]
        return not self['HostName'] == thisHost


    
    def isTargetRemote(self):
        """
        _isTargetRemote_

        Test if Target values are for a remote node

        Returns --

        - *Bool* : True if Target is on a remote host, false if on local
        host

        """
        thisHost = socket.gethostbyaddr(socket.gethostname())[0]
        return not self['TargetHostName'] == thisHost

    
    def __setitem__(self, key, value):
        """
        Override placement operator to make sure that base value 
        assignments are delegated to the appropriate call

        Args --

        - *key* : The Key to set the value of

        - *value* : The new value to be assigned to that key
        
        """
        if key in self._SetHandlers.keys():
            self._SetHandlers[key](value, False)
            self.update()
            return
        #  //
        # // delegate non base field assignment to normal
        #//  placement operator
        dict.__setitem__(self, key, value)
        self.update()
        return


    def __getitem__(self, key):
        """
        Intercept special keys and call the appropriate 
        handler method to get/construct/update the required value

        Args --

        - *key* : Name of the quantity to be retrieved
        
        """
        if key in self._GetHandlers.keys():
            return self._GetHandlers[key]()
        #  //
        # // delegate other keys to normal dict interface
        #//
        return dict.__getitem__(self, key)

    #  //======Set Handler methods========
    # // 
    #//
    def _MBSetBaseName(self, value, dependency = False):
        """
        Set the BaseName of the object. Accepts a full path
        or just a basename.

        Args: --

        - *value* : New value of BaseName

        - *dependency* : Bool indicating wether this set command comes
        from directly setting this field or from another set method
        
        """
        if value == None:
            dict.__setitem__(self, 'BaseName', None)
            return
        basename = os.path.basename(value)
        dict.__setitem__(self, 'BaseName', basename)
        dirname = os.path.dirname(value)
        if not dependency:
            if len(dirname) != 0:
                self._MBSetPathName(dirname, True)
        return

    def _MBSetPathName(self, value, dependency = False):
        """
        Set the PathName attribute, accepts a path name that
        includes environment variables, relative paths etc.
        Does not extract a BaseName value from the path

        Args: --

        - *value* : New value of PathName attribute

        - *dependency* : Bool indicating wether this set command comes
        from directly setting this field or from another set method
        
        """
        if value == None:
            dict.__setitem__(self, 'PathName', None)
            return
        dict.__setitem__(self, 'PathName', value)
        

    def _MBSetHostName(self, value, dependency = False):
        """
        Set Method for HostName
        """
        dict.__setitem__(self, 'HostName', value)
            

    def _MBSetAbsName(self, value, dependency = False):
        """
        Set Method for AbsName
        """
        if value == None:
            dict.__setitem__(self, 'AbsName', None)
            return
        abspath = self._ExpandPath(value)
        dict.__setitem__(self, "AbsName", abspath)
        if not dependency:
            pathname = os.path.dirname(value)
            basename = os.path.basename(value)
            self._MBSetBaseName(basename, True)
            self._MBSetPathName(pathname, True)
        return

    


    def _MBSetRemoteName(self, value, dependency = False):
        """
        Set Method for RemoteName
        """
        if value == None:
            dict.__setitem__(self, 'RemoteName', None)
            return
        nodename = re.split(':', value)[0]
        thepath =  re.split(':', value, 1)[1]
        if not dependency:
            basename = os.path.basename(thepath)
            pathname = os.path.dirname(thepath)
            self._MBSetBaseName(basename, True)
            self._MBSetPathName(pathname, True)
            self._MBSetAbsName(thepath, True)
            self._MBSetHostName(nodename, True)
        return
             
    
    def _MBSetTargetBaseName(self, value, dependency = False):
        """
        Set Method for TargetBaseName
        """
        if value == None:
            dict.__setitem__(self, 'TargetBaseName', None)
            return
        basename  =  os.path.basename(value)
        dict.__setitem__(self, 'TargetBaseName', basename)
        dirname = os.path.dirname(value)
        if not dependency:
            if len(dirname) != 0:
                self._MBSetTargetPathName(dirname, True)
        return


    
    def _MBSetTargetPathName(self, value, dependency = False):
        """
        Set Method for TargetPathName
        """
        if value == None:
            dict.__setitem__(self, 'TargetPathName', None)
            return
        dict.__setitem__(self, 'TargetPathName', value)

    
    def _MBSetTargetHostName(self, value, dependency = False):
        """
        Set Method for TargetHostName
        """

        dict.__setitem__(self, 'TargetHostName', value)

    def _MBSetTargetAbsName(self, value, dependency = False):
        """
        Set Method for TargetAbsName
        """
        if value == None:
            dict.__setitem__(self, 'TargetAbsName', None)
            return
        abspath = self._ExpandPath(value)
        dict.__setitem__(self, "TargetAbsName", abspath)
        if not dependency:
            pathname = os.path.dirname(value)
            basename = os.path.basename(value)
            self._MBSetTargetBaseName(basename, True)
            self._MBSetTargetPathName(pathname, True)
        return


    def _MBSetTargetRemoteName(self, value, dependency = False):
        """
        Set Method for TargetRemoteName
        """
        if value == None:
            dict.__setitem__(self, 'TargetRemoteName', None)
            return
        nodename = re.split(':', value)[0]
        thepath =  re.split(':', value, 1)[1]
        if not dependency:
            basename = os.path.basename(thepath)
            pathname = os.path.dirname(thepath)
            self._MBSetTargetBaseName(basename, True)
            self._MBSetTargetPathName(pathname, True)
            self._MBSetTargetAbsName(thepath, True)
            self._MBSetTargetHostName(nodename, True)
        return

    def _MBSetSourceBaseName(self, value, dependency = False):
        """
        Set Method for SourceBaseName
        """
        if value == None:
            dict.__setitem__(self, 'SourceBaseName', None)
            return
        basename = os.path.basename(value)
        dict.__setitem__(self, 'SourceBaseName', basename)
        dirname = os.path.dirname(value)
        if not dependency:
            if len(dirname) != 0:
                self._MBSetSourcePathName(dirname, True)
        return


    
    def _MBSetSourcePathName(self, value, dependency = False):
        """
        Set Method for SourcePathName
        """
        if value == None:
            dict.__setitem__(self, 'SourcePathName', None)
            return
        dict.__setitem__(self, 'SourcePathName', value)

    
    def _MBSetSourceHostName(self, value, dependency = False):
        """
        Set Method for SourceHostName
        """
        dict.__setitem__(self, 'SourceHostName', value)

    def _MBSetSourceAbsName(self, value, dependency = False):
        """
        Set Method for SourceAbsName
        """
        if value == None:
            dict.__setitem__(self, 'SourceAbsName', None)
            return
        abspath = self._ExpandPath(value)
        dict.__setitem__(self, "SourceAbsName", abspath)
        if not dependency:
            pathname = os.path.dirname(value)
            basename = os.path.basename(value)
            self._MBSetSourceBaseName(basename, True)
            self._MBSetSourcePathName(pathname, True)
        return


    def _MBSetSourceRemoteName(self, value, dependency=False):
        """
        Set Method for SourceRemoteName
        """
        if value == None:
            dict.__setitem__(self, 'SourceRemoteName', None)
            return
        nodename = re.split(':', value)[0]
        thepath =  re.split(':', value, 1)[1]
        if not dependency:
            basename = os.path.basename(thepath)
            pathname = os.path.dirname(thepath)
            self._MBSetSourceBaseName(basename, True)
            self._MBSetSourcePathName(pathname, True)
            self._MBSetSourceAbsName(thepath, True)
            self._MBSetSourceHostName(nodename, True)
        return

        
        

    #  //
    # //  End SetHandlers
    #//==================================




    #  //====Get Handler Methods=========
    # //
    #//
    def _MBGetFilesize(self):
        """
        Get Method for FileSize
        """
        pass

    def _MBGetHostName(self):
        """
        Get Method for HostName
        """
        hostname = dict.__getitem__(self, 'HostName')
        if hostname == None:
            hostname = socket.gethostbyaddr(socket.gethostname())[0]
            dict.__setitem__(self, 'HostName', hostname)
        return hostname
    

    def _MBGetAbsName(self):
        """
        Get Method for AbsName
        """
        if self['BaseName'] == None:
            return None
        if self['PathName'] == None:
            return self['BaseName']
        
        return os.path.join(self._ExpandPath(self['PathName']),
                            self['BaseName'])

    def _MBGetRemoteName(self):
        """
        Get Method for RemoteName
        """
        absname = self['AbsName']
        if absname == None:
            return None
        return "%s:%s" % (self['HostName'], absname)
    


    def _MBGetTargetAbsName(self):
        """
        Get Method for TargetAbsName
        """
        if self['TargetBaseName'] == None:
            return None
        if self['TargetPathName'] == None:
            return self['TargetBaseName']
        return os.path.join(self._ExpandPath(self['TargetPathName']),
                            self['TargetBaseName'])
    
    def _MBGetTargetRemoteName(self):
        """
        Get Method for TargetRemoteName
        """
        absname = self['TargetAbsName']
        hostname = self['TargetHostName']
        if absname == None:
            return None
        if hostname == None:
            return None
        return "%s:%s" % (self['TargetHostName'], absname)

    def _MBGetTarget(self):
        """
        Get Method for Target
        """
        absname = self['TargetAbsName']
        hostname = self['TargetHostName']
        if absname == None:
            return None
        if hostname == None:
            return absname
        return self['TargetRemoteName']


    def _MBGetSourceAbsName(self):
        """
        Get method for SourceAbsName key
        """
        if self['SourceBaseName'] == None:
            return None
        if self['SourcePathName'] == None:
            return self['SourceBaseName']
        return os.path.join(self._ExpandPath(self['SourcePathName']), 
                            self['SourceBaseName'])
    
    def _MBGetSourceRemoteName(self):
        """
        Get method for SourceRemoteName key
        """
        absname = self['SourceAbsName']
        hostname = self['SourceHostName']
        if absname == None:
            return None
        if hostname == None:
            return None
        return "%s:%s" % (self['SourceHostName'], absname)

    def _MBGetSource(self):
        """
        Get method for Source key
        """
        absname = self['SourceAbsName']
        hostname = self['SourceHostName']
        if absname == None:
            return None
        if hostname == None:
            return absname
        return self['SourceRemoteName']

        
    
    def _ExpandPath(self, path):
        """
        Util method to resolve relative, ~user and $VAR paths.

        Args --

        - *path* : Pathname string to be expanded

        Returns --

        - *string* : Path with variables expanded
        
        """
        if path == None:
            return path
        exppath = os.path.expandvars(os.path.expanduser(path))
        return os.path.abspath(exppath)
    
    def __repr__(self):
        return "<MetaBroker Instance: %s>" % self['RemoteName']
    
