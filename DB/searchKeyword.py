
##################
#
# 28 NOV 2018
# Returns a list of movies affiliated with the supplied keyword
#
##################


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

# parse the input keyword
keyword = sys.argv[1]

# get semicolon-separated list of movies for the keyword
c.execute("SELECT Movies from tblKeywords WHERE Keyword = '" + keyword + "'")
conn.commit()

result = c.fetchone()

if result:  # if there is at least one match (movie)

    movies = result[0]
    # print(movies)

    listmovies = [str(x) for x in movies.split(";") if x != ""]  # make array of movieIDs

    where_arg = "("
    for movie in listmovies:
        where_arg += "'" + movie + "',"
    where_arg = where_arg[:-1]
    where_arg += ")"  # remove the last comma

    c.execute("SELECT MovieName FROM tblMovies WHERE MovieID in " + where_arg + ";")
    conn.commit()
    result = c.fetchall()
    for movie in result:
        print(movie[0])

else:  # if no movies matched the keyword
    print("No movies matched the term '" + keyword + "'")
