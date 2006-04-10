#!/usr/bin/env python
"""
TaskObject Environment container
"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Environment.py,v 1.1 2005/12/30 18:46:35 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os
import re
import types


#  //
# // Module wide utils
#//
_PathSepRE = re.compile("%s" % os.pathsep)



class EnvVar(list):
    """
    _EnvVar_

    Environment variable holder. Deals with single values and path like
    values. Values are stored as a list, and the name of the variable
    is accessible through the name attribute
    
    """
    def __init__(self, varname = None, *vals):
        list.__init__(self)
        self.name = varname
        for item in vals:
            self.append(item)

    def _ProcessValue(self, vals):
        """
        _ProcessValue_

        Internal method that converts a value into a list if it
        is a path like value.
        Strings are checked for the path seperator, tuples are converted
        to lists, and a list of values is returned
        
        """
        if type(vals) == types.StringType:
            if not _PathSepRE.search(vals):
                return [vals]
            else:
                result = []
                splitVals = vals.split(os.pathsep)
                for item in splitVals:
                    if len(item) > 0:
                        result.append(item)
                return result
        elif type(vals) == types.ListType:
            return vals
        elif type(vals) == types.TupleType:
            return list(vals)
        else:
            return [vals]

    def append(self, val):
        """
        _append_

        Override list.append to check value is not a path of values
        Append value to list of process values.
        """
        for item in self._ProcessValue(val):
            list.append(self, item)
                    
    def __str__(self):
        """
        formatted string repr.
        """
        result = ''
        for item in self:
            if result == '':
                result = '%s' % item
                continue
            elif result[-1] == ':':
                result = '%s%s' % (result, item)
                continue
            result = '%s%s%s' % (result, os.pathsep, item)
        return result
    
    def set(self, *value):
        """
        _set_

        Set the value of this instance to those provided in argument
        Remove old values then set provided list as values.
        """
        del self[0:len(self)]
        for item in value:
            self.append(item)

    def prepend(self, value):
        """
        _prepend_
        
        Insert value at the front of the values list, Ie at the
        beginning of a path
        """
        tmp = self._ProcessValue(value)
        tmp.reverse()
        for item in tmp:
            self.insert(0, item)

    def isVariable(self):
        """
        _isVariable_
        
        Returns true if this envvar is a single value variable
        """
        return (len(self) == 1)
            
    def isPath(self):
        """
        _isPath_
        
        Returns true if variable is a multi value path.
        """
        return (len(self) > 1)



class Environment(dict):
    """
    _Environment_
    
    Environment container object for storing an environment
    setup in a generic way. Contents are mapped by name to an
    instance of EnvVar objects.
    """
    def __init__(self):
        dict.__init__(self)
        self._Order = []

    def __setitem__(self, key, value):
        """
        _setitem_

        Overide key assignment operator to only accept EnvVar instances
        and keep track of the ordering
        """
        if not isinstance(value, EnvVar):
            msg = "Value: %s for key: %s not instance of EnvVar" % (key,
                                                                    value)
            print "TODO: Throw Exception"
            raise RuntimeError,msg
        if key not in self._Order:
            self._Order.append(key)
        dict.__setitem__(self, key, value)


    def addVariable(self, name, *values):
        """
        _addVariable_
        
        Add a variable to the environment reference.
        Creates EnvVar instance internally.

        Args --

        - *name* : name of env var

        - *values* : list of values. Multiple values get treated as a path
        like construct
        
        """
        ev1 = EnvVar(name, *values)
        self[name] = ev1
        return

    def __str__(self):
        """
        formatted string repr.
        """
        result = ""
        for item in self._Order:
            result += "%s\n" % str(item)
        return result
    

    def keys(self):
        """
        _keys_

        Override basic dict method to keep ordered list of keys
        """
        return self._Order


