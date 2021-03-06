# this creates synonyms & grants permissions to the WRITER & READER accounts
# associated with the oracle instances used for T0AST


import cx_Oracle

# most of the accounts use these as their suffix
readerSuffix="READER"
writerSuffix="WRITER"

# the development instance
#DBInstance="DEVDB10"
#Account="CMS_T0AST"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""

# a user development instance
#DBInstance="DEVDB10"
#Account="CMS_T0AST_GOWDY"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""
#readerSuffix="R"
#writerSuffix="W"

# the integration instance
#DBInstance="INT2R_LB"
#Account="CMS_T0ASTIT"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""

# the production instance(s)
#DBInstance="CMS_T0AST"
#Account="CMS_T0AST"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""

# scale test database
#DBInstance="int9r_lb"
#Account="CMS_T0AST_SCALE"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""
#readerSuffix="R"
#writerSuffix="W"

# prodtest instance 1
#DBInstance="CMS_T0AST"
#Account="CMS_T0AST_1"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""

# prodtest instance 2
#DBInstance="CMS_T0AST"
#Account="CMS_T0AST_2"
#adminDBPass=""
#readerDBPass=""
#writerDBPass=""

DBInstance="CMS_T0AST"
Account="CMS_T0AST"
adminDBPass=""
readerDBPass=""
writerDBPass=""

cx_Oracle.threaded=True

con = cx_Oracle.connect(user=Account,password=adminDBPass,dsn=DBInstance)
cur = con.cursor()
cur.execute("select table_name from user_tables")
tables = cur.fetchall()
print tables
cur.execute("select sequence_name from user_sequences")
sequences = cur.fetchall()
print sequences
cur.execute("select object_name from user_objects where object_type = 'FUNCTION'")
functions = cur.fetchall()
print functions

user = "%s_%s" % (Account, writerSuffix)
print user
for t in tables:
 print t
 cur.execute("Grant all on %s to %s" % (t[0], user))

for s in sequences:
 print s
 cur.execute("Grant alter,select on %s to %s" % (s[0], user))

for f in functions:
 print f
 cur.execute("Grant execute on %s to %s" % (f[0], user))
 
cur.execute("Grant select on all_tab_columns to %s" % (user))
cur.execute("Grant select on all_constraints to %s" % (user))
cur.execute("Grant select on all_cons_columns to %s" % (user))

con.commit()

user = "%s_%s" % (Account, readerSuffix)
print user
for t in tables:
 print t
 cur.execute("Grant select on %s to %s" % (t[0], user))

for s in sequences:
 print s
 cur.execute("Grant select on %s to %s" % (s[0], user))

for f in functions:
 print f
 cur.execute("Grant execute on %s to %s" % (f[0], user))
 
cur.execute("Grant select on all_tab_columns to %s" % (user))
cur.execute("Grant select on all_constraints to %s" % (user))
cur.execute("Grant select on all_cons_columns to %s" % (user))

con.commit()
con.close()


AccountName="%s_%s" % (Account,writerSuffix)
con = cx_Oracle.connect(user=AccountName,password=writerDBPass,dsn=DBInstance)
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
for f in functions:
 try:
  cur.execute("Drop synonym %s " % f[0])
 except:
  print "%s is crap" % f[0]
con.commit()

for t in tables:
 cur.execute("Create synonym %s for %s.%s" % (t[0],Account, t[0]))

for s in sequences:
 cur.execute("Create synonym %s for %s.%s" % (s[0],Account, s[0]))

for f in functions:
 cur.execute("Create synonym %s for %s.%s" % (f[0],Account, f[0]))

con.commit()
con.close()

AccountName="%s_%s" % (Account,readerSuffix)
con = cx_Oracle.connect(user=AccountName,password=readerDBPass,dsn=DBInstance)
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
for f in functions:
 try:
  cur.execute("Drop synonym %s " % f[0])
 except:
  print "%s is crap" % f[0]
con.commit()

for t in tables:
 cur.execute("Create synonym %s for %s.%s" % (t[0],Account, t[0]))

for s in sequences:
 cur.execute("Create synonym %s for %s.%s" % (s[0],Account, s[0]))

for f in functions:
 cur.execute("Create synonym %s for %s.%s" % (f[0],Account, f[0]))
 
con.commit()
con.close()

