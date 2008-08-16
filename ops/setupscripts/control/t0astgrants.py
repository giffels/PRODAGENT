# this creates synonyms & grants permissions to the WRITER & READER accounts
# associated with the oracle instances used for T0AST


import cx_Oracle

# the development instance
DBInstance="DEVDB10"
adminDBPass="PlumJ4m791"
writerDBPass="m4rm4l4d3"
# the production instance
#DBInstance=CMS_T0AST
#adminDBPass="PlumJ4m791"
#writerDBPass="PlumJ4m791"

cx_Oracle.threaded=True


connectString="CMS_T0AST/%s@%s" % (adminDBPass,DBInstance)
con = cx_Oracle.connect(connectString)
cur = con.cursor()
cur.execute("select table_name from user_tables")
tables = cur.fetchall()
print tables
cur.execute("select sequence_name from user_sequences")
sequences = cur.fetchall()
print sequences
user = "CMS_T0AST_WRITER"
print user
for t in tables:
 print t
 cur.execute("Grant all on %s to %s" % (t[0], user))

for s in sequences:
 print s
 cur.execute("Grant alter,select on %s to %s" % (s[0], user))

cur.execute("Grant select on all_tab_columns to %s" % (user))
cur.execute("Grant select on all_constraints to %s" % (user))
cur.execute("Grant select on all_cons_columns to %s" % (user))

con.commit()

user = "CMS_T0AST_READER"
print user
for t in tables:
 print t
 cur.execute("Grant select on %s to %s" % (t[0], user))

for s in sequences:
 print s
 cur.execute("Grant select on %s to %s" % (s[0], user))

cur.execute("Grant select on all_tab_columns to %s" % (user))
cur.execute("Grant select on all_constraints to %s" % (user))
cur.execute("Grant select on all_cons_columns to %s" % (user))

con.commit()
con.close()




connectString="CMS_T0AST_WRITER/%s@%s" % (writerDBPass,DBInstance)
con = cx_Oracle.connect(connectString)
cur = con.cursor()

for t in tables:
 try:
  cur.execute("Drop synonym %s " % t[0])
 except:
  print "%s is crap" % t[0]
for s in sequences:
 try:
  cur.execute("Drop synonym %s " % s[0])
 except:
  print "%s is crap" % s[0]
con.commit()

for t in tables:
 cur.execute("Create synonym %s for CMS_T0AST.%s" % (t[0], t[0]))

for s in sequences:
 cur.execute("Create synonym %s for CMS_T0AST.%s" % (s[0], s[0]))

con.commit()
con.close()
