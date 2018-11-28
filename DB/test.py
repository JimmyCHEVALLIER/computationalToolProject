
import sqlite3
import os
import sys

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists
if not os.path.isfile(dbname):
    #raise ValueError("Error! The database does not exist")
    sys.exit("Error! The database does not exist")

# if the database exists, then continue
conn = sqlite3.connect(dbname)
c = conn.cursor()




c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
conn.commit()
results = c.fetchall()
#print(results)
for movie in results:
    print(movie[0], movie[1])
