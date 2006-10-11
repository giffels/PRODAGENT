from ProdAgentDB import Session

def hasURL(url):
    sqlStr="""SELECT COUNT(*) FROM pm_cooloff WHERE url="%s";
        """ %(url)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if rows[0][0]==0:
        return False
    return True

def insert(url,delay="00:00:00"):
    sqlStr="""INSERT INTO pm_cooloff(url,delay) 
        VALUES("%s","%s"); """ %(url,delay)
    Session.execute(sqlStr)

def remove():
   sqlStr="""DELETE FROM pm_cooloff WHERE 
       ADDTIME(log_time,delay)<= CURRENT_TIMESTAMP """
   Session.execute(sqlStr)
