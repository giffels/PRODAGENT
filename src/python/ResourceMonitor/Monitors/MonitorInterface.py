#!/usr/bin/env python
"""
_MonitorInterface_

Interface class for Monitor plugins.

Interface is pretty simple:

Override the Call method to return a ResourceConstraint instance,
which is the number of resources available for jobs and constraints.

The PluginConfig mechanism is used for this as well, so you can read
dynamic parameters from self.pluginConfig


"""

import logging
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ResourceConstraint import ResourceConstraint

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB


class MonitorInterface:
    """
    _MonitorInterface_
    
    
    Abstract Interface Class for Resource Monitor Plugins

    """
    def __init__(self):
        self.activeSites = []
        self.allSites = {}
        self.siteThresholds = {}
        self.siteAttributes = {}
        self.retrieveSites()
        self.pluginConfiguration = None
        

    def newConstraint(self):
        """
        _newConstraint_

        Factory method, returns a new, empty constraint

        """
        return ResourceConstraint()


    def retrieveSites(self):
        """
        _retrieveSites_

        Return a list of all  sites from the ResourceControl DB
        and stores them in this object for access by the plugins
        
        """
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        
        resCon = ResourceControlDB()
        siteNames = resCon.siteNames()
        
        for site in siteNames:
            siteData = resCon.getSiteData(site)
            self.allSites[site] = siteData
            if siteData['Active'] == True:
                self.activeSites.append(site)
            siteIndex = siteData['SiteIndex']
            self.siteThresholds[site] = resCon.siteThresholds(siteIndex)
            self.siteAttributes[site] = resCon.siteAttributes(siteIndex)
            

        del resCon
        
        Session.commit_all()
        Session.close_all()        
        return 
    

    

    def __call__(self):
        """
        _operator()_

        Override this method to make whatever callouts you need to
        determine that you have resources available

        Should return a list of ResourceConstraint instances that will be
        published as  ResourceAvailable events
        """
        return [ResourceConstraint() ]

    
