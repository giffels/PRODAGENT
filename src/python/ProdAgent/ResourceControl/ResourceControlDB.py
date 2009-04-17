#!/usr/bin/env python
"""
_ResourceControlDB_

DB API for ResourceControl tables

"""


from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.Core.ProdException import ProdException


"""
Session.set_database(dbConfig)
Session.connect()
Session.start_transaction()

resCon = ResourceControlDB()
resCon.doDBStuff()
del resCon

self.ms.commit()
Session.commit_all()
Session.close_all()
"""


class ResConDBError(ProdException):
    """
    _ResConDBError_

    Exception class for ResourceControlDB Errors

    """
    def __init__(self, msg, **data):
        ProdException.__init__(self, msg, 6000, **data)
        

class ResourceControlDB:
    """
    _ResourceControlDB_

    Object that provides DB table interface to the ResourceControl Tables
    Requires a Session to be established before construction

    """
    def __init__(self):
        pass



    def siteNames(self):
        """
        _siteNames_

        Get a list of site names

        """
        sqlStr = """ SELECT site_name FROM rc_site;"""
        Session.execute(sqlStr)
        results = Session.fetchall()
        result = [ x[0] for x in results]
        return result
                   
    
    def getSiteData(self, siteName):
        """
        _getSiteData_

        Get all site data for the siteName provided

        """
        sqlStr = """ 
        SELECT site_index, ce_name, se_name, is_active
           FROM rc_site WHERE site_name="%s";
        """ % siteName

        Session.execute(sqlStr)
        results = Session.fetchone()
        if results == None:
            return None
        
        siteData = {
            "SiteName" : siteName,
            "SiteIndex" : results[0],
            "CEName" : results[1],
            "SEName" : results[2],
            "Active" : results[3],

            }
        if siteData['Active'] == "true":
            siteData['Active'] = True
        else:
            siteData['Active'] = False
            
        return siteData
    
        
    def siteMatchData(self):
        """
        _siteMatchData_

        Get a list of all sites. Each list entry is a dictionary
        of SiteIndex, SiteName, SEName

        """
        sqlStr = """ 
        SELECT site_index AS SiteIndex,
               site_name AS SiteName,
               se_name AS SEName
             FROM rc_site;
        """
        
        Session.execute(sqlStr)
        results = Session.fetchall()
        siteMatch = []
        [ siteMatch.append({
            "SiteIndex" : x[0],
            "SiteName"  : x[1],
            "SEName"    : x[2],
            }) for x in results ]
        
        return siteMatch
    
    

    def newSite(self, siteName, seName, ceName, isActive = True):
        """
        _newSite_

        Add a new site to the DB
        
        """
        if ceName is None:
            ceName = "NULL"
        else:
            ceName = "\"%s\"" % ceName
        
        sqlStr = """INSERT into rc_site
            (site_name, se_name, ce_name, is_active)
              VALUES ( "%s", "%s", %s, """ % (
            siteName, seName, ceName,
            )

        activeValue = "false"
        if isActive:
            activeValue = "true"

        sqlStr += " \"%s\");" % activeValue
        try:
            dbCur = Session.get_cursor()
            dbCur.execute(sqlStr)
            dbCur.execute("SELECT LAST_INSERT_ID()")
            siteIndex = dbCur.fetchone()[0]
        except AssertionError, ex:
            msg = "Failed to insert new site: %s\n" % siteName
            msg += "Probable Duplication of siteName\n"
            msg += str(ex)
            raise ResConDBError(msg, SiteName = siteName)
        except Exception, ex:
            msg = "Failed to insert new Site: %s\n" % siteName
            msg += str(ex)
            raise ResConDBError(msg, SiteName = siteName)
            
        return siteIndex
        

    def dropSite(self, siteName):
        """
        _dropSite_

        Remove Site configuration from DB

        """
        sqlStr = """DELETE FROM rc_site WHERE site_name="%s";""" % siteName
        Session.execute(sqlStr)
        return


    def deactivate(self, siteName):
        """
        _deactivate_

        Set a site to inactive status

        """
        sqlStr = """UPDATE rc_site SET is_active="false" WHERE site_name = "%s";""" % siteName
        Session.execute(sqlStr)
        return


    def activate(self, siteName):
        """
        _activate_

        Set a site to active status

        """
        sqlStr = """UPDATE rc_site SET is_active="true" WHERE site_name = "%s";""" % siteName
        Session.execute(sqlStr)
        return
        

    def siteThresholds(self, siteIndex):
        """
        _siteThresholds_

        get a dictionary of all thresholds for a site.

        """
        sqlStr = """ SELECT threshold_name, threshold_value
              FROM rc_site_threshold
                WHERE site_index=%s;""" % siteIndex
        Session.execute(sqlStr)
        result = Session.fetchall()

        resultDict = {}
        [ resultDict.__setitem__(x[0], x[1]) for x in result ]
        return resultDict
        
        
    def updateThresholds(self, siteIndex, **thresholds):
        """
        _updateThresholds_

        update the threshold settings with the values in the args dictionary
        provided.
        
        """
        sqlStr = """ INSERT INTO rc_site_threshold
            (site_index, threshold_name, threshold_value) VALUES """

        
        valStrs = [ "(%s,\"%s\", %s)" % (
            siteIndex, x[0], x[1]) for x in thresholds.items() ]

        vals = ""
        for item in valStrs:
            vals += "%s," % item
        vals = vals[:-1]
        sqlStr += vals
        
        sqlStr += "ON DUPLICATE KEY UPDATE "
        sqlStr += "threshold_value = VALUES(threshold_value);"

        Session.execute(sqlStr)
        return

    def siteAttributes(self, siteIndex):
        """
        _siteAttributes_

        retrieve a dictionary of site attributes for the site with the
        index provided
        
        """
        sqlStr = """ SELECT attr_name, attr_value
              FROM rc_site_attr
                WHERE site_index=%s;""" % siteIndex
        Session.execute(sqlStr)
        result = Session.fetchall()

        resultDict = {}
        [ resultDict.__setitem__(x[0], x[1]) for x in result ]
        return resultDict

    
    def updateAttributes(self, siteIndex, **attributes):
        """
        _updateAttributes_

        Update the attributes of a site by index

        """
        
        sqlStr = """ INSERT INTO rc_site_attr
            (site_index, attr_name, attr_value) VALUES """

        
        valStrs = [ "(%s,\"%s\", \"%s\")" % (
            siteIndex, x[0], x[1]) for x in attributes.items() ]

        vals = ""
        for item in valStrs:
            vals += "%s," % item
        vals = vals[:-1]
        sqlStr += vals
        
        sqlStr += "ON DUPLICATE KEY UPDATE "
        sqlStr += "attr_value = VALUES(attr_value);"
        Session.execute(sqlStr)
        return
    
