"""
Class to define the standardised formatting of MySQL results.
"""
import datetime
import time
from  sqlalchemy.engine import RowProxy
from WMCore.Database.DBFormatter import DBFormatter

class MySQLBase(object):
    def __init__(self, logger, dbinterface):
        self.logger = logger
        self.dbi = dbinterface
        self.wmformatter = DBFormatter(self.logger, self.dbi)


    def truefalse(self, value):
        if value in ('False', 'FALSE', 'n', 'NO', 'No'):
            value = 0
        return bool(value)

    def convertdatetime(self, t):
        return int(time.mktime(t.timetuple()))

    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return self.convertdatetime(t)

    def format(self, result, dictionary = False):
        """
        Some standard formatting
        """
        if not dictionary:
            return self.wmformatter.format(result)
        else:
            return self.wmformatter.formatDict(result)
        
#        out = []
#        for r in result:
#           if dictionary == False:
#            for i in r.cursor.fetchall():
#                out.append(i)
#           else:
#        
#             for i in r.cursor.fetchall():
#               row = RowProxy(r,i)
#               out.append(dict(row.items()))
#   
#               
#        return out
    
    def formatOne(self, result, dictionary = False):
        """
        single value format

        """
        if not dictionary:
            return self.wmformatter.formatOne(result)
        else:
            return self.wmformatter.formatOneDict(result)
      
#        if len(result) == 0:
#            return [] 
#        value = result[0].fetchone()
#        if value == None:
#            return []
#
#        if dictionary == True:
#           row = RowProxy(result[0],value)
#           value = dict(row.items())
#        return value


    def getBinds(self):
        """
        Return the appropriately formatted binds for the sql
        """
        return {}

    def execute(self, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(),
                         conn = conn, transaction = transaction)
        return self.format(result)

       
    def restoreCaps(self, dicts, *newkeys):
        """
        Utility function to restore capitalization in dictionary keys
         lost to conform with Oracle - use of this should be phased out
        """
        for dict in dicts:
            for key in newkeys:
                dict[key] = dict.pop(key.lower())
        return dicts