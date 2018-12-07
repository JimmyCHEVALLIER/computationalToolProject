
# 04/12-18
# MapReduce inline tester script

import sqlite3
import datetime
import uuid
#from mapReduceInline import calcTfidf  # tester
from mapReduce import TFIDF  # import MRJob class

# connect to db
conn = sqlite3.connect("movieFinderTestDb.sqlite")
c = conn.cursor()

# get data from db
c.execute("SELECT movieid, movieplot FROM tblMovies WHERE movieid NOT IN (SELECT DISTINCT movieid FROM tblKeywords);")
conn.commit()
results = c.fetchall()  # format of items in results list: (movieid, movieplot) | each list item = a movie
totalMovies = len(results)

# write data from db to file, which serves as the input to the mrjob
open('mrJobInputTest.txt', 'w').close()  # clear the file
with open("mrJobInputTest.txt", "a") as f:
    for item in results:
        f.write(item[0] + "|||" + item[1].replace("\n", " ") + "|||" + str(totalMovies) + "\n")  #item0 = movieID, item1 = movieplot

#inputfile = 'mrJobInputTest.txt'  # name of file containing input - tester
inputfile = 'mrJobInputTest.txt'  # name of file containing input
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

print("start time:", starttime)
print("end time:", endtime)
print("time it took to run the MRJob:", endtime - starttime)
print("items in output:", len(output))

#for item in output:
#    print(item[0][0])

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
# -------- test results ------- #

MRJob v1 (original first version that worked):
time it took to run the MRJob: 0:00:33.952875
items in output: 21

MRjob v2 (revised the mapreduce):
time it took to run the MRJob: 0:00:16.871350
items in output: 36

MRJob v3 (added isalpha() check):
time it took to run the MRJob: 0:00:16.946212
items in output: 27

MRJob v4 (added stopword check):
time it took to run the MRJob: 0:00:22.310290
items in output: 21

MRJob v5 (removed stopword check and set tfidf threshold to 15):
time it took to run the MRJob: 0:00:23.601844
items in output: 7

MRJob v6 (set tfidf threshold to 12):
time it took to run the MRJob: 0:00:21.190394
items in output: 11

mrjob v7 (set tfidf threshold to 11):
time it took to run the MRJob: 0:00:17.370609
items in output: 13

set tfidf to 12:
time it took to run the MRJob: 0:00:17.308145
items in output: 11

set tfidf to 13:
time it took to run the MRJob: 0:00:17.070122
items in output: 8

set tfidf to 15:
time it took to run the MRJob: 0:00:18.240287
items in output: 7


"""