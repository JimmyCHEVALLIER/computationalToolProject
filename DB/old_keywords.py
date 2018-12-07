
"""""""""""""""""""""

DATE: 13 NOV 2018
DESCRIPTION: 

Script to find all keywords
for each movie and store
them in the table tblKeywords.

Tools:

    * NLP (new tool)
    * MapReduce

"""""""""""""""""""""

import sqlite3
import os
import sys
import ast
import uuid
import datetime

# ------------ CONNECT TO DB ------------ #

dbname = open("db_name.txt", "r").read()  # get the name of the db
print("-----> Using db:", dbname)

# check if db exists
if not os.path.isfile(dbname):
    #raise ValueError("Error! The database does not exist")
    sys.exit("Error! The database does not exist")

# if the database exists, then continue
conn = sqlite3.connect(dbname)
c = conn.cursor()

# get all movies from the db that not not have keywords in the db
c.execute("SELECT movieid, movieplot FROM tblMovies WHERE movieid NOT IN (SELECT DISTINCT movieid FROM tblKeywords);")
conn.commit()
results = c.fetchall()
#print(results[0])
print("----->", len(results), "movie(s) will be processed")
results = str(results)  # store all the results from the db into a string


# insert the results into a temporary table
f = open("mrJobInput.txt", "w")
f.write(results)
f.close()

beforeMRexecute = datetime.datetime.now()  # make note on time, used to see execution time for MRJob

# execute mrjob via terminal and output to file
#strExecute = "python3 mapReduce.py < '" + results + "' > keywords.txt"
#os.system(strExecute)
os.system("python3 mapReduceNew.py mrJobInput.txt > keywords.txt")

afterMRexecute = datetime.datetime.now()  # make note on time, used to see execution time for MRJob
print("-----> Time it took to execute MRJob:", afterMRexecute - beforeMRexecute)

# drop the index on the keywords col
c.execute("DROP INDEX IF EXISTS index_keywords")
conn.commit()

# insert keywords in keyword table
f = open("keywords.txt", "r")
for line in f:
    id, value = line.split("\t")
    value = float(value) # tfidf value
    id = ast.literal_eval(id)  # transform string to literal array
    word = str(id[0])
    movie = str(id[1])

    #print("movie =>", movie, "| word =>", word, "| tfidf =>", value)  # debugging
    # print("Inserting...", word)
    c.execute("INSERT INTO tblKeywords VALUES ('" + str(uuid.uuid4()) + "', '" + word + "', '" + movie + "', " + str(value) + ");")
    conn.commit()
f.close()

# create index on the keyword column again
c.execute("CREATE INDEX index_keywords on tblKeywords(Keyword);")
conn.commit()
conn.close()
