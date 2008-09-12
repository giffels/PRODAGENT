#!/usr/bin/env python
"""
_Persistency_

Persistency methods for MetaBroker dictionaries


"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery



def save(mbInstance):
    """
    _save_

    Convert the MB instance into an IMProvNode tree

    """
    saveDict = dict(mbInstance)
    result = IMProvNode("MetaBroker")
    for key, val in saveDict.items():
        if val == None:
            continue
        node = IMProvNode("Entry", str(val), Key = str(key))
        result.addNode(node)
    return result


def load(improvNode, mbInstance):
    """
    _load_

    load data from IMProvNode into mbInstance provided

    """
    entryQuery = IMProvQuery("/MetaBroker/Entry")
    entries = entryQuery(improvNode)
    for entry in entries:
        key = entry.attrs.get("Key", None)
        if key == None:
            continue
        value = str(entry.chardata)
        mbInstance[str(key)] = value
    return mbInstance

                          
    

