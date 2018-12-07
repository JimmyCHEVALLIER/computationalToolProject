

"""""""""""""""""""""

DATE: 13 NOV 2018
DESCRIPTION: 

Script to clear 
all tables in the db.

"""""""""""""""""""""

import sqlite3
import sys
import os

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists and ensure we do not just connect to an in-memory database
if not os.path.isfile(dbname):
    sys.exit("Error! The database does not exist")

conn = sqlite3.connect(dbname)
c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()

for table in tables:
    table = table[0]  # result is tuple
    c.execute("DELETE FROM %s" % table)

conn.commit()
