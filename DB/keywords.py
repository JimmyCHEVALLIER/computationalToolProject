
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
results = str(c.fetchall())  # store all the results from the db into a string
#print("result from db =>", results)

# insert the results into a temporary table
f = open("mrJobInput.txt", "w")
f.write(results)
f.close()


#mr_job = TFIDF()  # pass the string as input to the MRjob
#runner = mr_job.make_runner()
#runner.run()
#runner.stream_output()
#for line in runner.stream_output():
    #key, value = mr_job.parse_output_line(line)
    #print(key, value)

# execute mrjob via terminal and output to file
os.system("python3 mapReduce.py mrJobInput.txt > keywords.txt")

# drop the index on the keywords col
c.execute("DROP INDEX IF EXISTS index_keywords")
conn.commit()

# read each line from the file and insert the keyword in the database
# filtering here? Only take top 10 words or words over a certain score?
f = open("keywords.txt", "r")
for line in f:
    id, value = line.split("\t")
    value = float(value)  # cast tfidf value as float
    id = ast.literal_eval(id)  # transform string to literal array
    word = str(id[0])
    movie = str(id[1])

    if value > 4.0:
        #print("movie =>", movie, "| word =>", word, "| tfidf =>", value)  # debugging

        c.execute("SELECT * FROM tblKeywords WHERE Keyword = '" + word + "';")
        conn.commit()
        result = c.fetchone()
        if result:  # if the keyword already exists in the keywords table
            print("Updating...", word)
            c.execute("UPDATE tblKeywords SET Movies = Movies || '" + movie + ";' WHERE Keyword = '" + word + "';")  # update the row
        else:
            #print("INSERT INTO tblKeywords(Keyword, Movies) VALUES ('" + word + "', '" + movie + ";');")  # debugging
            print("Inserting...", word)
            c.execute("INSERT INTO tblKeywords(Keyword, Movies) VALUES ('" + word + "', '" + movie + ";');")  # else insert new row
        conn.commit()
f.close()

# iterate over the cursor itself
# THIS IS NOT GOING TO WORK AS THERE IS NO WAY OF COUNTING HOW MANY DOCUMENTS CONTAIN EACH WORD
# IF WE EXECUTE THE MRJOB FOR EACH MOVIE ONE AT A TIME

"""
c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
for row in c:
    movieID = row[0]
    moviePlot = row[1]
    #print("movieID =", movieID)  # debugging
    #print("moviePlot =", moviePlot)  # debugging
    #print(movieID + "||" + moviePlot)  # debugging

    #result = tfidf(movieID + "||" + moviePlot)
    #print(result)
    tfidf(movieID + "||" + moviePlot)
"""

# INSTEAD OF EXECUTING THE MRJOB FOR ONE MOVIE AT A TIME, WE CAN STORE ALL THE
#c.execute("SELECT MovieID, MoviePlot FROM tblMovies;")
#conn.commit()
#results = c.fetchall()
#print("results: ", results)
#print("number of movies =", len(results))
#keywords = TFIDF(results) # function to calculate the TF-IDF value for each word in the movie plot
#print(keywords)

# finish off by adding index to the keyword column
c.execute("CREATE INDEX index_keywords on tblKeywords(Keyword);")
conn.commit()
conn.close()



"""

# get list of movies and their keywords
select tblMovies.moviename, tblkeywords.keyword 
from tblmovies 
inner join tblkeywords 
    on tblmovies.movieid = replace(tblkeywords.movies,";","") 
order by moviename, keyword;

"""