# this creates synonyms & grants permissions to the WRITER & READER accounts
# associated with the oracle instances used for T0AST


import cx_Oracle

# the development instance
#DBInstance="DEVDB10"
#Account="CMS_T0AST"
#adminDBPass=""
#writerDBPass=""
# the production instance(s)
#DBInstance="CMS_T0AST"
#Account="CMS_T0AST"
#adminDBPass=""
#writerDBPass=""

#DBInstance="CMS_T0AST"
#Account="CMS_T0AST_1"
#adminDBPass=""
#writerDBPass=""

DBInstance="CMS_T0AST"
Account="CMS_T0AST_2"
adminDBPass=""
writerDBPass=""

# the integration instance (at least for now)

cx_Oracle.threaded=True


connectString="%s/%s@%s" % (Account,adminDBPass,DBInstance)
con = cx_Oracle.connect(connectString)
cur = con.cursor()
cur.execute("select table_name from user_tables")
tables = cur.fetchall()
print tables
cur.execute("select sequence_name from user_sequences")
sequences = cur.fetchall()
print sequences
user = "%s_WRITER"%Account
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

user = "%s_READER"%Account
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




connectString="%s_WRITER/%s@%s" % (Account,writerDBPass,DBInstance)
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
 cur.execute("Create synonym %s for %s.%s" % (t[0],Account, t[0]))

for s in sequences:
 cur.execute("Create synonym %s for %s.%s" % (s[0],Account, s[0]))

con.commit()
con.close()
