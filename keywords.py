
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

# TODO: check that the system has all necessary packages installed

import sqlite3
import os.path
import sys
from mapReduce import TFIDF

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists
if not os.path.isfile(dbname):
    #raise ValueError("Error! The database does not exist")
    sys.exit("Error! The database does not exist")


# if the database exists, then continue
conn = sqlite3.connect(dbname)
c = conn.cursor()


# function to calculate the TF-IDF value for each word in the movie plot
def tfidf(mrjob):
    result = TFIDF(mrjob)
    return result


# iterate over the cursor itself
c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
for row in c:
    movieID = row[0]
    moviePlot = row[1]
    #print("movieID =", movieID)  # debugging
    #print("moviePlot =", moviePlot)  # debugging
    #print(movieID + "||" + moviePlot)
    result = tfidf(movieID + "||" + moviePlot)
    print(result)

conn.close()

