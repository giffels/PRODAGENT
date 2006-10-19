#!/usr/bin/env python
"""
_ResourceConstraint_

Util object for building a Resources Available Payload that uses
constraints on

- number of jobs
- job type
- workflow
- site

"""


class ResourceConstraint(dict):
    """
    _ResourceConstraint_

    

    """
    def __init__(self):
        self.setdefault("count", 1)
        self.setdefault("type", None)
        self.setdefault("workflow", None)
        self.setdefault("site", None)



    def __str__(self):
        """
        format object into constraint string that can be used as
        payload to ResourcesAvailableByConstraint event

        """
        result = "count=%s;" % self['count']
        if self['type'] != None:
            result += "type=%s;" % self['type']
        if self['workflow'] != None:
            result += "workflow=%s;" % self['workflow']
        if self['site'] != None:
            result += "site=%s;" % self['site']
        
        return result

    def parse(self, strForm):
        """
        _parse_

        Convert the string provided into values in this object

        """
        if strForm == "":
            # default case: treat as single unconstrained resources
            return

        if strForm.strip().isdigit():
            # is a plain integer, treat as multiple unconstrained resource
            count = int(strForm)
            self['count'] = count
            return
        
        constraints = strForm.split(";")
        
        for constraint in constraints:
            if constraint.startswith("count="):
                try:
                    value = int(constraint.split("count=")[1])
                    self['count'] = value
                except Exception:
                    continue
            elif constraint.startswith("type="):
                self['type'] = constraint.split("type=")[1]

            elif constraint.startswith("site="):
                self['site'] = constraint.split("site=")[1]
            elif constraint.startswith("workflow="):
                self['workflow'] = constraint.split("workflow=")[1]
            

        return
    
        

        
        
