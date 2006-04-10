#!/usr/bin/env python
"""
_CoreQueries_

Basic protocol based query implementations

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: CoreQueries.py,v 1.1 2005/12/30 18:51:40 evansde Exp $"



import os
import commands

from MB.query.BaseQuery import BaseQuery



class SimpleQuery(BaseQuery):
    """
    _SimpleQuery_

    Basic local existence check that uses os.path.exists
    
    """

    def query(self, mbInstance):
        """
        override query to do a simple os.path.exists
        test on AbsName field
        """
        if os.path.exists(mbInstance['AbsName']):
            self._Exists = True
        return

class DCAPQuery(BaseQuery):
    """
    _DCAPQuery_

    Basic dcache existence check that uses os.path.exists

    """

    def query(self, mbInstance):
        """
        override query to do a simple os.path.exists
        test on AbsName field
        """
        if os.path.exists(mbInstance['AbsName']):
            self._Exists = True
        return

class RFIOQuery(BaseQuery):
    """
    _RFIOQuery_
    Perform a query based on rfio

    """
    def query(self, mbInstance):
        """
        Perform a query based on rfio
        """

        if mbInstance["HostName"] == 'null' or mbInstance["HostName"] == "" :
            comm = "rfstat %s" % (mbInstance["AbsName"])
        else :
            comm = "rfstat %s:%s" % (
                mbInstance["HostName"],
                mbInstance["AbsName"],
                )
        val = commands.getstatusoutput(comm)[0]
        try:
            val = int(val)
            if val == 0:
                self._Exists = True
                return
            else:
                self._Exists = False
                return
        except StandardError:
            self._Exists = False
            return
     

class RSHQuery(BaseQuery):
    """
    _RSHQuery_

    Remote shell based existence check

    """

    def query(self, mbInstance):
        """
        Perform a query based on rsh
        """
        comm = "/usr/kerberos/bin/rsh %s" % (
            mbInstance["HostName"],
            )
        comm += " \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["AbsName"],
            )
        val = commands.getstatusoutput(comm)[0]
        try:
            val = int(val)
            if val == 0:
                self._Exists = True
                return 
            else:
                self._Exists = False
                return
        except StandardError:
            self._Exists = False
            return 

class SSHQuery(BaseQuery):
    """
    _SSHQuery_

    Secure Shell base existence check

    """

    def query(self, mbInstance):
        """
        Perform an existence check using ssh
        """
        comm = "ssh %s \'( [ -e %s ] && exit 0 ) || exit 1\'" % (
            mbInstance["HostName"],
            mbInstance["AbsName"],
            )
        val = commands.getstatusoutput(comm)[0]
        #print "ssh output:",val
        try:
            val = int(val)
            if val == 0:
                self._Exists = True
                return 
            else:
                self._Exists = False
                return
        except StandardError:
            self._Exists = False
            return 



