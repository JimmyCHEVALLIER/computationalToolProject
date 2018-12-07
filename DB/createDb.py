

"""""""""""""""""""""

DATE: 13 NOV 2018
DESCRIPTION: 

Script to create sqlite db 
for the final project in
Computational Tools for
Data Science.

Tools:

    * SQl and databases

"""""""""""""""""""""


# --- tables in db --- #
# movies: movieID (uuid created in db), movie name, production year, plot, budget, box office
# actors: actorID (uuid created in the db), actor name
# directors: directorID (uuid create in the db), director name
# movie2actor: ID, movie id, actor id (bridge table)

# some tables are bridges in case of multi:multi relationships

# --- indices in db --- #
# tblKeywords.Keyword - to quickly find a keyword in the column and its movies

import sqlite3
import sys
import os

dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists and ensure we do not just connect to an in-memory database
if not os.path.isfile(dbname):
    sys.exit("Error! The database does not exist")

conn = sqlite3.connect(dbname)  # creates the db if it does not already exist
c = conn.cursor()

# create needed tables
c.execute("CREATE TABLE tblMovies "
          "("
          "MovieID TEXT PRIMARY KEY, "
          "MovieName TEXT, "
          "ReleaseDate DATE, "
          "MoviePlot TEXT, "
          "Budget INTEGER, "
          "BoxOffice INTEGER"
          ");")

c.execute("CREATE TABLE tblActors "
          "("
          "ActorID TEXT PRIMARY KEY, "
          "ActorName TEXT"
          ");")

c.execute("CREATE TABLE tblDirectors "
          "("
          "DirectorID TEXT PRIMARY KEY, "
          "DirectorName TEXT"
          ");")

c.execute("CREATE TABLE tblBridge_Movie2Actor "
          "("
          "ID TEXT PRIMARY KEY, "
          "MovieID TEXT, "
          "ActorID TEXT, "
          "FOREIGN KEY(MovieID) REFERENCES tblMovies(MovieID), "
          "FOREIGN KEY(ActorID) REFERENCES tblActors(ActorID)"
          ");")

c.execute("CREATE TABLE tblKeywords "
          "("
          "ID TEXT PRIMARY KEY, "
          "Keyword TEXT, "  # put an index on this column
          "MovieID TEXT, "  # contains a movieID. This means that a movie will have several rows in this table
          "tfidfVal REAL"  # contains the tfidf value for the keyword in the movie
          ");")

# storing information from python objects in db:
# https://stackoverflow.com/questions/2047814/is-it-possible-to-store-python-class-objects-in-sqlite

#c.execute("CREATE INDEX index_keywords on tblKeywords(Keyword);")

conn.commit()
print("Created the movie database!")
conn.close()
