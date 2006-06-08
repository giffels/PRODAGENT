#!/usr/bin/env python
"""
_TrivialFileCatalog_

Object to contain LFN to PFN mappings from a Trivial File Catalog
and provide functionality to match LFNs against them

Usage:

given a TFC file, invoke readTFC on it. This will return
a TrivialFileCatalog instance that can be used to match LFNs
to PFNs.

Usage: Given a TFC constact string: trivialcatalog_file:/path?protocol=proto


    filename = tfcFilename(tfcContactString)
    protocol = tfcProtocol(tfcContactString)
    tfcInstance = readTFC(filename)

    lfn = "/store/PreProd/unmerged/somelfn.root"

    pfn = tfcInstance.matchLFN(protocol, lfn)


"""

import os
import re
import urlparse

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery



class TrivialFileCatalog(list):
    """
    _TrivialFileCatalog_

    Object that can map LFNs to PFNs based on contents of a Trivial
    File Catalog
    """

    def __init__(self):
        list.__init__(self)
        self.preferredProtocol = None # attribute for preferred protocol
        

    def addMapping(self, protocol, match, result):
        """
        _addMapping_

        Add an lfn to pfn mapping to this instance

        """
        entry = {}
        entry.setdefault("protocol", protocol)
        entry.setdefault("path-match-expr", match)
        entry.setdefault("path-match", re.compile(match))
        entry.setdefault("result", result)
        self.append(entry)


    def matchLFN(self, protocol, lfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match
        """
        for mapping in self:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match'].match(lfn):
                try:
                    splitLFN = mapping['path-match'].split(lfn, 1)[1]
                except IndexError:
                    continue
                result = mapping['result'].replace("$1", splitLFN)
                return result

        return None

    def __str__(self):
        result = ""
        for item in self:
            result += "LFN-to-PFN: %s %s %s\n" % (
                item['protocol'], item['path-match-expr'], item['result']
                )
        return result
    

def tfcProtocol(contactString):
    """
    _tfcProtocol_

    Given a Trivial File Catalog contact string, extract the
    protocol from it.

    """
    args = urlparse.urlsplit(contactString)[3]
    value = args.replace("protocol=", '')
    return value

def tfcFilename(contactString):
    """
    _tfcFilename_

    Extract the filename from a TFC contact string.

    """
    value = contactString.replace("trivialcatalog_file:", "")
    path = urlparse.urlsplit(value)[2]
    return path

    
            
def readTFC(filename):
    """
    _readTFC_

    Read the file provided and return a TrivialFileCatalog
    instance containing the details found in it

    """
    if not os.path.exists(filename):
        msg = "TrivialFileCatalog not found: %s" % filename
        raise RuntimeError, msg


    try:
        node = loadIMProvFile(filename)
    except StandardError, ex:
        msg = "Error reading TrivialFileCatalog: %s\n" % filename
        msg += str(ex)
        raise RuntimeError, msg

    query = IMProvQuery("storage-mapping/lfn-to-pfn")
    mappings = query(node)

    tfcInstance = TrivialFileCatalog()

    for mapping in mappings:
        protocol = mapping.attrs.get("protocol", None)
        match = mapping.attrs.get("path-match", None)
        result = mapping.attrs.get("result", None)
        if True in (protocol, match, mapping == None):
            continue
        tfcInstance.addMapping(str(protocol), str(match), str(result))
        
    return tfcInstance

