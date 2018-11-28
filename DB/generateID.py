import sqlite3
import uuid

def generateID():
    return str(uuid.uuid4())  # generate random uuid

"""
con = sqlite3.connect("movieFinderDb.sqlite")
con.create_function("generateID", 0, generateID())
cur = con.cursor()
cur.execute("select generateID()")
print(cur.fetchone()[0])
"""

print(generateID())
