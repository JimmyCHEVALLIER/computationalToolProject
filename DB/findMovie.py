
"""

03 DEC 2018
Returns a list of movies affiliated with the supplied keywords, actors and/or directors
Arguments syntax: keyword1 keyword2 actor:'actor name' director:'director name'

"""

# todo: use generator to parse result from db instead of storing result

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

# build list of args
numArgs = len(sys.argv) - 1  # first arg is the python script
print(numArgs, "args(s) passed")

keywords = []
actors = []
directors = []

for i in range(numArgs):
    currentArg = sys.argv[i + 1]
    if 'actor' not in currentArg and 'director' not in currentArg:
        keywords.append("'" + currentArg + "'")
    elif 'actor' in currentArg:
        actors.append("'" + currentArg.replace("actor:", "") + "'")
    else:
        directors.append("'" + currentArg.replace("director:", "") + "'")

#print("keywords:", keywords)  # debugging
#print("actors:", actors)  # debugging
#print("directors:", directors)  # debugging


def addSqlKeyword(keyword):
    return "SELECT MovieID " \
           "FROM tblKeywords " \
           "WHERE Keyword LIKE " + keyword  # use LIKE wildcard to allow searching in all lower case


def addSqlActor(actor):
    return "SELECT tblMovies.MovieID " \
           "FROM tblMovies " \
           "INNER JOIN tblBridge_Movie2Actor " \
           "ON tblMovies.MovieID = tblBridge_Movie2Actor.MovieID " \
           "INNER JOIN tblActors " \
           "ON tblActors.ActorID = tblBridge_Movie2Actor.ActorID " \
           "WHERE tblActors.ActorName LIKE " + actor  # use LIKE wildcard to allow searching in all lower case

def addSqlDirector(director):
    return "SELECT tblMovies.MovieID " \
           "FROM tblMovies " \
           "INNER JOIN tblBridge_Movie2Director " \
           "ON tblMovies.MovieID = tblBridge_Movie2Director.MovieID " \
           "INNER JOIN tblDirectors " \
           "ON tblDirectors.DirectorID = tblBridge_Movie2Director.DirectorID " \
           "WHERE tblDirectors.DirectorName LIKE " + director  # use LIKE wildcard to allow searching in all lower case

# build the sql query
if len(keywords) > 0:
    subsql = addSqlKeyword(keywords[0])
    keywords.pop(0)  # remove the inserted item
elif len(actors) > 0:
    subsql = addSqlActor(actors[0])
    actors.pop(0)  # remove the inserted item
elif len(directors) > 0:
    subsql = addSqlDirector(directors[0])
    directors.pop(0)  # remove the inserted item

#print(subsql)  # debugging

for keyword in keywords:
    subsql += " INTERSECT " + addSqlKeyword(keyword)

for actor in actors:
    subsql += " INTERSECT " + addSqlActor(actor)

for director in directors:
    subsql += " INTERSECT " + addSqlDirector(director)

# parse the input keyword and build the SQL query
sql = "SELECT MovieName " \
      "FROM tblMovies " \
      "WHERE MovieID IN (" + subsql + ") " \
      "ORDER BY MovieName;"

# print(sql)  # debugging
c.execute(sql)
conn.commit()

result = c.fetchall()

if result:  # if there is at least one match (movie)

    print(len(result), "movie(s) found!")
    for movie in result:
        print(movie[0])
        #pass

else:  # if no movies matched the keyword
    print("No movies found")
