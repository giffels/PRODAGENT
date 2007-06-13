#!/usr/bin/env python
"""
_ResourceControlAPI_

Basic API calls for components and plugins to access information
from the ResourceControlDB

"""

import logging
from  ProdAgent.ResourceControl.ResourceControlDB import ResourceControlDB

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

Session.set_database(dbConfig)



def allSiteData():
    """
    _allSiteData_

    Method to get all known site data

    """
    Session.connect()
    Session.start_transaction()
    resourceControlDB = ResourceControlDB()
    _SiteList = resourceControlDB.siteNames()
    siteData = [ resourceControlDB.getSiteData(x) for x in _SiteList ]
    return siteData

def activeSiteData():
    """
    _activeSiteData_

    Return a list of dictionaries for all active sites containing
    SE, CE, Site Name, Site Index

    """
    siteData = allSiteData()
    siteData = [ x for x in siteData if x['Active'] == True ]
    return siteData


def thresholds(site = None):
    """
    _thresholds_

    Retrieve the thresholds for a specified site or
    all active sites if the site arg isnt provided

    """
    Session.connect()
    Session.start_transaction()
    resourceControlDB = ResourceControlDB()
    sites = []
    if site != None:
        siteData = resourceControlDB.getSiteData(site)
        if siteData == None:
            return {}
        sites.append(siteData)
    else:
        sites = activeSiteData()
        

    result = {}

    [ result.__setitem__(
        x['SiteName'], resourceControlDB.siteThresholds(x['SiteIndex']))
      for x in sites ]

    return result
    
def attributes(site = None):
    """
    _attributes_

    Retrieve the attributes for a specified site or all active sites
    if the site arg isnt provided

    """
    Session.connect()
    Session.start_transaction()
    resourceControlDB = ResourceControlDB()
    sites = []
    if site != None:
        siteData = resourceControlDB.getSiteData(site)
        if siteData == None:
            return {}
        sites.append(siteData)
    else:
        sites = activeSiteData()
        
        
    result = {}

    [ result.__setitem__(
        x['SiteName'], resourceControlDB.siteAttributes(x['SiteIndex']))
      for x in sites ]
    
    return result
    
        
def createSiteNameMap(activeOnly = True):
    """
    _createSiteNameMap_

    For the set of Active sites, generate a dictionary mapping
    SiteIndex: SiteName
    SEName: SiteName
    CEName: SiteName

    """
    if activeOnly:
        siteData = activeSiteData()
    else:
        siteData = allSiteData()

    siteMap = {}
    for site in siteData:
        siteMap[site['SiteIndex']] = site['SiteName']
        siteMap[site['CEName']] = site['SiteName']
        siteMap[site['SEName']] = site['SiteName']
    return siteMap

def createSiteIndexMap(activeOnly = True):
    """
    _createSiteIndexMap_

    For the set of Active sites, generate a dictionary mapping
    SiteName: SiteIndex
    SEName: SiteIndex
    CEName: SiteIndex

    """
    if activeOnly:
        siteData = activeSiteData()
    else:
        siteData = allSiteData()

    siteMap = {}
    for site in siteData:
        siteMap[site['SiteName']] = site['SiteIndex']
        siteMap[site['CEName']] = site['SiteIndex']
        siteMap[site['SEName']] = site['SiteIndex']
    return siteMap



def createCEMap(activeOnly = True):
    """
    _createCEMap_

    For the set of Active sites, generate a dictionary mapping 
    SiteName : CEName 
    SEName: CEName
    SiteIndex: CEName

    Useful for submitter plugings to translate sites to CEs
    
    """
    if activeOnly:
        siteData = activeSiteData()
    else:
        siteData = allSiteData()

    ceMap = {}
    for site in siteData:
        ceMap[site['SiteIndex']] = site['CEName']
        ceMap[site['SiteName']] = site['CEName']
        ceMap[site['SEName']] = site['CEName']

    return ceMap


def createSEMap(activeOnly = True):
    """
    _createSEMap_

    For the set of Active sites, generate a dictionary mapping
    SiteName : SEName
    CEName : SEName
    SiteIndex : SEName

    """
    if activeOnly:
        siteData = activeSiteData()
    else:
        siteData = allSiteData()
    seMap = {}
    for site in siteData:
        seMap[site['SiteIndex']] = site['SEName']
        seMap[site['SiteName']] = site['SEName']
        seMap[site['CEName']] = site['SEName']

    return seMap



def knownSite(siteId):
    """
    _knownSite_

    Return the site index if the site Id is known, None if
    not.

    This method searches for site Id as:
    1. An Index
    2. A site name
    3. An SE name
    4. A CE name

    """
    
    
    siteIndices = createSiteIndexMap(True)
    if siteId in siteIndices.values():
        # Is a site index
        return siteId
    if siteId in siteIndices.keys():
        return siteIndices[siteId]
    return None


