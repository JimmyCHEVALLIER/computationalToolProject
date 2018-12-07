
"""""""""""""""""""""

DATE: 04 DEC 2018
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
import uuid
import datetime
from mapReduce import TFIDF  # import MRJob class

# get the db name
dbname = open("db_name.txt", "r").read()  # get the name of the db
print("-----> Using db:", dbname)

# check if db exists
if not os.path.isfile(dbname):
    sys.exit("Error! The database does not exist")

# if the database exists, then continue
conn = sqlite3.connect(dbname)
c = conn.cursor()

# get data from db
c.execute("SELECT movieid, movieplot FROM tblMovies WHERE movieid NOT IN (SELECT DISTINCT movieid FROM tblKeywords);")
conn.commit()
results = c.fetchall()  # format of items in results list: (movieid, movieplot) | each list item = a movie
totalMovies = len(results)
print("----->", totalMovies, "movie(s) will be processed")

# write data from db to file, which serves as the input to the mrjob
open('mrJobInput.txt', 'w').close()  # clear the file
with open("mrJobInput.txt", "a") as f:
    for item in results:
        f.write(item[0] + "|||" + item[1].replace("\n", " ") + "|||" + str(totalMovies) + "\n")  #item0 = movieID, item1 = movieplot

inputfile = 'mrJobInput.txt'  # name of file containing input
output = []

# todo: turn this into a generator?
# run the mrjob programmatically
mr_job = TFIDF(args=[inputfile, '-r', 'local'])  # pass input as the first arg
with mr_job.make_runner() as runner:
    starttime = datetime.datetime.now()
    runner.run()
    endtime = datetime.datetime.now()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
        output.append((key, value))


print("-----> start time:", starttime)
print("-----> end time:", endtime)
print("-----> time it took to run the MRJob:", endtime - starttime)
print("-----> items in output:", len(output))

# drop the index on the keywords col
c.execute("DROP INDEX IF EXISTS index_keywords")
conn.commit()

# insert keywords and tfidf value into db
for item in output:
    term = item[0][0]
    movieID = item[0][1]
    tfidfVal = item[1]
    c.execute("INSERT INTO tblKeywords "
              "VALUES ('" + str(uuid.uuid4()) + "', '" + term + "', '" + movieID + "', " + str(tfidfVal) + ");")
conn.commit()

# recreate index on the keyword column
c.execute("CREATE INDEX index_keywords on tblKeywords(Keyword);")
conn.commit()
conn.close()



"""
EXECUTION RESULTS

threshold = 12.0, executed on mac
-----> start time: 2018-12-05 00:32:50.769653
-----> end time: 2018-12-05 00:36:33.086376
-----> time it took to run the MRJob: 0:03:42.316723
-----> items in output: 140716

threshold = 12.0, executed on mac
-----> start time: 2018-12-06 09:54:59.549989
-----> end time: 2018-12-06 09:58:33.263176
-----> time it took to run the MRJob: 0:03:33.713187
-----> items in output: 140716

threshold = 11.0, executed on mac
-----> start time: 2018-12-06 10:01:11.045846
-----> end time: 2018-12-06 10:04:47.978221
-----> time it took to run the MRJob: 0:03:36.932375
-----> items in output: 155436

threshold = 10.0, executed on mac
-----> start time: 2018-12-06 10:13:42.372022
-----> end time: 2018-12-06 10:17:00.858624
-----> time it took to run the MRJob: 0:03:18.486602
-----> items in output: 173545

threshold = 8.0, executed on mac
-----> start time: 2018-12-06 12:06:22.672401
-----> end time: 2018-12-06 12:09:40.866823
-----> time it took to run the MRJob: 0:03:18.194422
-----> items in output: 300309

threshold = 7.0, executed on mac
-----> start time: 2018-12-06 12:12:19.325666
-----> end time: 2018-12-06 12:15:41.730532
-----> time it took to run the MRJob: 0:03:22.404866
-----> items in output: 411101

threshold = 6.0, executed on mac
-----> start time: 2018-12-06 12:18:34.813684
-----> end time: 2018-12-06 12:22:07.498506
-----> time it took to run the MRJob: 0:03:32.684822
-----> items in output: 582115


"""