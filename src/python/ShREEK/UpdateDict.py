# pylint: disable-msg=W0613
#
# Disable pylint warning about unused arguments to methods
# for the defaultUpdate method
"""
_UpdateDict_

Dictionary Implementation that allows keys to have an updator method
associated with them so that the value of the key can be updated
by calling the method and using its return value.

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: UpdateDict.py,v 1.1 2005/12/30 18:54:25 evansde Exp $"
__author__ = "evansde@fnal.gov"

from types import InstanceType, FunctionType, MethodType


def defaultUpdate(state):
    """
    Default method returns string 'NotImplemented'.
    """
    return "NotImplemented"


class UpdateDict(dict):
    """
    _UpdateDict_

    Update dictionary class.
    Dictionary Implementation with update handlers that
    can be registered for certain keys. Normal keys
    can be added in the usual way using the [] operator.
    Keys to be updated can be added using the addUpdateKey
    method and providing a updator function.
    
    """

    def __init__(self):
        dict.__init__(self)
        self._Updators = {}
        self._UpdatorOrder = []


    def addUpdateKey(self, key, value, updator=None):
        """
        _addUpdateKey_
        
        Add update key and set value. Specify the method used
        to update the key by reference.

        Args --

        - *key* : The name of the dictionary key

        - *value* : The initial value of the new key

        - *updator* : Function reference to be called to update the
        key. It needs to accept one argument, which is an instance
        of this UpdateDict, and needs to return the new value to
        be assigned to the key.
        
        """
        if not self._Updators.has_key(key):
            if updator == None:
                self._Updators[key] = defaultUpdate
            else:
                self._CheckUpdator(updator)
                self._Updators[key] = updator
            
        self._UpdatorOrder.append(key)      
        dict.__setitem__(self, key, value)
        return
    
    def doUpdate(self):
        """
        _doUpdate_

        Call the updator methods to update the keys that they
        are registered for.
        Exceptions are not caught so that they can be propagated
        up to whatever is calling doUpdate
        """
        for key in self._UpdatorOrder:
            
            if  not self._Updators.has_key(key):
                continue
            if type(self._Updators[key]) == MethodType:
                dict.__setitem__(self, key, self._Updators[key](self))
            else:
                dict.__setitem__(self, key,
                                 self._Updators[key].__call__(self))
        return
    

    def _CheckUpdator(self, updator):
        """
        Check updator type for updator specified.
        Raises a TypeError if updator is not a callable object,
        Ie: Method, Function of object with __call__ method.
        
        """
        updType = type(updator)
        if updType == FunctionType:
            return
        if updType == MethodType:
            return
        if updType == InstanceType:
            if callable(updator):
                raise
        raise TypeError, "updator is not a callable type: %s" % updator
    


