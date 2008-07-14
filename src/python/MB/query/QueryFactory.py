#!/usr/bin/env python
"""
_Query Factory_

Module containing loader and manager tools for
dealing with object existence query tests for MetaBrokers

"""
__revision__ = "$Id: QueryFactory.py,v 1.1 2005/12/30 18:51:40 evansde Exp $"
__version__ = "$Revision: 1.1 $"


from MB.MBException import MBException
from MB.MBException import FactorySingleton
from MB.MetaBroker import MetaBroker


from MB.query.BaseQuery import BaseQuery
from MB.query.CoreQueries import SimpleQuery
from MB.query.CoreQueries import RSHQuery
from MB.query.CoreQueries import SSHQuery
from MB.query.CoreQueries import RFIOQuery
from MB.query.CoreQueries import DCAPQuery

def getQueryFactory():
    """
    Returns singleton QueryFactory object
    """
    single = None
    try:
        return QueryFactory()
    except FactorySingleton,singleton:
        single = singleton.instance()
    return single



class QueryFactory(dict):
    """
    _QueryFactory_

    Singleton factory for managing query methods.
    Derived from a dictionary, a map of protocol name
    to handler instance is stored. Basic handlers
    for local, rsh and ssh are installed by
    default, others can be registered via
    the registerQueryMethod interface.

    Do not import this object directly, use the
    getQueryFactory method provided

    """
    __singleton = None


    def __init__(self):
        if ( self.__singleton is not None ):
            instance = QueryFactory.__singleton
            raise FactorySingleton(instance)
        QueryFactory.__singleton = self
        dict.__init__(self)
        self.registerQueryMethod(None, SimpleQuery())
        self.registerQueryMethod("local", SimpleQuery())
        self.registerQueryMethod("rsh", RSHQuery())
        self.registerQueryMethod("ssh", SSHQuery())
        self.registerQueryMethod("rfio:", RFIOQuery())
        self.registerQueryMethod("dcap:", DCAPQuery())
        

    def __getitem__(self, mbOrKey):
        if isinstance(mbOrKey, MetaBroker):
            key = mbOrKey.get('QueryMethod', None)
        else:
            key = mbOrKey
        return dict.__getitem__(self, key)
    

    def __setitem__(self, key, value):
        self.registerQueryMethod(key, value)

    def registerQueryMethod(self, name, handler):
        """
        _registerQueryMethod_

        Register a query handler for a particular
        protocol.

        Args --

        - *name* : Protocol name (eg rsh, ssh etc)

        - *handler* : specialised instance of BaseQuery
        object to perform the query
        """
        if not isinstance(handler, BaseQuery):
            msg = "Non Query instance registered "
            msg += "with QueryFactory"
            raise MBException(
                "Transport Factory Error: %s" % msg,
                ModuleName = "MB.query.QueryFactory",
                MethodName = "registerQueryMethod",
                Handler=handler,
                HandlerName=name)
        dict.__setitem__(self, name, handler)
        return

