#!/usr/bin/env python

"""
Unittest Session module
"""


import logging
import time
import unittest

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

class SessionUnitTests(unittest.TestCase):
    """
    TestCase for Session module 
    """

    def setUp(self):
        #logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.WARNING)

    def testA(self):
        print('Inserting workflows')
        try:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            for i in xrange(0,10):
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","123","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id"+str(i)))
                Session.execute(sqlStr)
            #now we will brake the session object deliberatly
            Session.session['default']['connection']=None
            Session.session['default']['cursor']=None

            sqlStr="""SELECT COUNT(*) FROM we_Workflow"""
            print('Case 1 (none type connection and cursor)*****************')
            Session.execute(sqlStr)
            rows=Session.fetchall()
            self.assertEqual(int(rows[0][0]),10)
            #now we will brake the session object again deliberatly
            Session.session['default']['connection']=None
            Session.session['default']['cursor']=None
            print('Case 2 (none type connection and cursor)****')
            Session.commit()

             
            #put in a query that is incorrect (should raise an error)
            print('Case 2 (malformed query)*********************************')
            try:
                sqlStr="""INSERT some garbage"""
                Session.execute(sqlStr)
            except Exception,ex:
                print("Error testing successful : "+str(ex))
            #put in a query that violates a db constraint (should raise an error)
            print('Case 3 (wellformed query with db constraint violation)***')
            try:
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","123","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id"+str(1)))
                Session.execute(sqlStr)
            except Exception,ex:
                print("Error testing successful : "+str(ex))

            for i in xrange(0,5):
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","234","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id_1_"+str(i)))
                Session.execute(sqlStr)
            # close the cursor
            Session.session['default']['cursor'].close()
            print('Case 4 (closing a cursor)*********************************')

            sqlStr="""SELECT COUNT(*) FROM we_Workflow"""
            Session.execute(sqlStr)
            rows=Session.fetchall()
            Session.commit()
            self.assertEqual(int(rows[0][0]),15)
            

            for i in xrange(0,5):
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","345","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id_2_"+str(i)))
                Session.execute(sqlStr)
            # close the connection
            Session.session['default']['connection'].close()
            print('Case 5 (closing a connection)******************************')
            sqlStr="""SELECT COUNT(*) FROM we_Workflow"""
            Session.execute(sqlStr)
            rows=Session.fetchall()
            self.assertEqual(int(rows[0][0]),20)
            # close the connection
            Session.session['default']['connection'].close()
            print('Case 6 (closing a connection)******************************')
            Session.commit()

            sqlStr="""DELETE FROM we_Workflow WHERE priority='234' """
            Session.execute(sqlStr)
            sqlStr="""DELETE FROM we_Workflow WHERE priority='123' """
            Session.execute(sqlStr)
            for i in xrange(0,50):
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","456","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id_3_"+str(i)))
                Session.execute(sqlStr)
            Session.session['default']['cursor'].close()
            Session.session['default']['connection'].close()
            print('Case 7 (closing connection and cursor)**********************')
            Session.commit()
            sqlStr="""SELECT COUNT(*) FROM we_Workflow"""
            Session.execute(sqlStr)
            rows=Session.fetchall()
            self.assertEqual(int(rows[0][0]),55)

            for i in xrange(0,50):
                sqlStr="""INSERT INTO we_Workflow(events_processed,id,owner,priority,prod_mgr_url) 
                VALUES("1000","%s","elmo","567","http://some.where.over.the.rainbow") 
                """ %(str("workflow_id_4_"+str(i)))
                Session.execute(sqlStr)
            Session.session['default']['connection'].close()
            print('Case 8 (closing connection)**********************')
            Session.rollback()
            sqlStr="""SELECT COUNT(*) FROM we_Workflow"""
            Session.execute(sqlStr)
            rows=Session.fetchall()
            self.assertEqual(int(rows[0][0]),55)
            
            

            
            Session.commit_all()
            Session.close_all()
        except StandardError, ex:
            msg = "Failed TestA:\n"
            msg += str(ex)
            self.fail(msg)

    
    def runTest(self):
        self.testA()
        
            
if __name__ == '__main__':
    unittest.main()
