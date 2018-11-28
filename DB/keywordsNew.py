
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
import os
import sys
import ast
# from script import class
#from .mapReduce import TFIDF
from mapReduce import TFIDF

# ------------ CONNECT TO DB ------------ #

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists
if not os.path.isfile(dbname):
    #raise ValueError("Error! The database does not exist")
    sys.exit("Error! The database does not exist")

# if the database exists, then continue
conn = sqlite3.connect(dbname)
c = conn.cursor()

# ------------- fetch movieIDs and movie plots from db ------------- #

#c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
#conn.commit()
#results = str(c.fetchall())  # store all the results from the db into a string
# stream this directly into the job runner?

# insert the results into a temporary table
#f = open("mrJobInput.txt", "w")
#f.write(results)
#f.close()

# iterator to get the movies and their plot one at a time
def generator_MoviePlot():
    c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
    conn.commit()
    results = c.fetchall()
    for movie in results:
        id = movie[0]
        plot = movie[1]
        yield id, plot


"""
# run the job programmatically  https://pythonhosted.org/mrjob/guides/runners.html#running-your-job-programmatically
# The '-' argument signifies that we use stdin.
mr_job = TFIDF(['--runner', 'inline', '-'])
stdin = generator_MoviePlot()
mr_job.stdin = stdin
results = []
with mr_job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
        results.append((key, value))
print(results)
"""


# test generator
movies = generator_MoviePlot()
print(next(movies))
print(next(movies))


#TFIDF.run()


