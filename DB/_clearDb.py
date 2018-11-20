

"""""""""""""""""""""

DATE: 13 NOV 2018
DESCRIPTION: 

Script to clear the db.

"""""""""""""""""""""

import sqlite3

dbname = open("db_name.txt", "r").read()  # get the name of the db

conn = sqlite3.connect(dbname)
c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()

for table in tables:
    table = table[0]  # result is tuple
    c.execute("DELETE FROM %s" % table)

conn.commit()
