import sqlite3

conn = sqlite3.connect('headphones.db')
conn.isolation_level = None
c = conn.cursor()

c.execute("begin")
c.execute("SELECT * from allalbums")
c.execute("commit")
print c.fetchone()
