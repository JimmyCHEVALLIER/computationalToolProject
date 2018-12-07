import sqlite3
import os.path
import sys
import uuid

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists
if not os.path.isfile(dbname):
    sys.exit("Error! The database does not exist")

conn = sqlite3.connect(dbname)  # creates the db if it does not already exist
c = conn.cursor()

# insert test data
c.execute("INSERT INTO tblMovies values ('" + str(uuid.uuid4()) + "', 'The Great Movie', NULL, 'This is a movie plot this movie is about a happy family who lives in the city', 10, 20);")
c.execute("INSERT INTO tblMovies values ('" + str(uuid.uuid4()) + "', 'Test Movie 1', NULL, 'Test movie plot this movie is about a fast train', 100, 604);")

conn.commit()

conn.close()



