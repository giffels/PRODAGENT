#!/usr/bin/env python
"""
Base Class for Query Objects for checking
existence of MetaBroker Objects
"""
__revision__ = "$id$"
__version__ = "$Revision: 1.1 $"




class BaseQuery:
    """
    _BaseQuery_

    Abstract Object for Checking the Existence
    of a MetaBroker object by looking at the fields
    contained within it to test existence of the object
    referenced and return true or false
    """
    def __init__(self):
        self._Exists = False
        self._PositionalArgs = []
        self._KeywordArgs = {}
        self._Reset()
        

    def _Reset(self):
        """
        Reset the state of the Query object
        after each query
        """
        self._Exists = False
        self._PositionalArgs = []
        self._KeywordArgs = {}

    def __call__(self, mbInstance, *args, **keywords):
        """
        _Operator()_

        Defines the action of a Query on a metabroker
        instance

        Args --

        - *mbInstance* : MetaBroker instance to be queried

        - *args* : Positional args
        
        - *kw* : keyword args

        Returns --
        
        - *bool* : True if file exists, False if not

        """
        self._PositionalArgs = args
        self._KeywordArgs = keywords
        self._Reset()
        self._Prepare(mbInstance)
        self.query(mbInstance)
        return self._Exists


    def _Prepare(self, mbInstance):
        """
        Call overriden prepare method
        """
        self.prepare(mbInstance)
        

    def prepare(self, mbInstance):
        """override to prep for query"""
        pass

    def query(self, mbInstance):
        """override to perform query"""
        pass
                
   
