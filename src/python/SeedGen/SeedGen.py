#!/usr/bin/env python
"""
_SeedGen_

Random Seed Generator module

 Usage from other python modules:

 from SeedGen import getSeedGen
 gen = getSeedGen()
 newseed1 = gen.createSeed()
 newseed2 = gen.createSeed()
 etc...

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: SeedGen.py,v 1.1 2006/03/08 22:48:16 evansde Exp $"

import exceptions
import os
import re
import socket
import whrandom


class SeedGenSingleton(exceptions.Exception):
    """
    Singleton Carrier exception
    """
    def __init__(self, instance):
        exceptions.Exception.__init__(self)
        self._Instance = instance

    def instance(self):
        """return the singleton instance"""
        return self._Instance


#  //
# // Ensure single instance of SeedGen class is used
#//  to create seeds to avoid duplicates
def getSeedGen():
    """
    Returns singleton SeedGen object
    """
    single = None
    try:
        return SeedGen()
    except SeedGen, excep:
        single = excep.instance()
    return single



#  //
# // 
#//
class SeedGen:
    """
    _SeedGen_
    
    Unique seed sequence generator object that
    uses process id, hardware address and whrandom
    to create a random 32 bit integer
    """
    __singleton = None

    #  //
    # // Max bit mask size for 32 bit integers
    #//
    _MAXINT = 2147483645
    
    def __init__(self):
        if ( self.__singleton is not None ):
            self = SeedGen.__singleton
            raise SeedGenSingleton(SeedGen.__singleton)
        #  //
        # // Use process Id
        #//
        self._pid = os.getpid()
        #  //
        # // Use IP address converted into 32 bit int
        #//
        self._ip = self._GetIPAddress()
        #  // 
        # // Keep track of last seed generated
        #//
        self._LastSeed = 0
        

        
    def createSeed(self):
        """
        _createSeed_

        Create a new unique seed and return it

        Returns --

        - *int* : Random 32 bit integer
        
        """
        seed = ( self._DoWhrandom() | self._ip | self._pid) & self._MAXINT
        while seed == self._LastSeed:
            seed = self.createSeed()
        self._LastSeed = seed
        return seed

 

    def _GetIPAddress(self):
        """
        Extract the MAC address or IP address and
        convert it to a 32 integer
        """
        try:
            reHWaddr = re.compile(
                "HWaddr ((([0-9A-F][0-9A-F]):){5}([0-9A-F][0-9A-F]))"
                )
            text = os.popen("/sbin/ifconfig eth0").read()
            hwaddr = re.sub(
                ":", "",
                reHWaddr.search(text).group(1)
                )
            val = '%s' % int(hwaddr, 16)
        except Exception:
            val = socket.gethostbyname(socket.gethostname())
            val = re.sub('\.', '', val)
            
        val = long(val)
        val = val & self._MAXINT
        return val
        
    def _DoWhrandom(self):
        """
        Create a random 32 bit integer using the
        system time as a seed
        """
        return whrandom.randint(1, self._MAXINT)
    



if __name__ == '__main__':
    seedgen = getSeedGen()
    for i in range(0, 100):
        print seedgen.createSeed()
