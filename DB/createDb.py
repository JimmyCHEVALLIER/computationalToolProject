

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
# movies: movie id, movie name, release date, movie plot, budget, box office
# keywords: id, keyword, movie id, tfidfVal
# actors: actor id, actor name
# directors: director id, director name
# movie2actor: id, movie id, actor id
# movie2director: id, movie id, director id

# --- indices in db --- #
# tblMovies (MovieID, MovieName) - to quickly find a movie by id and its name
# tblKeywords(Keyword) - to quickly find a keyword in the column and its movies
# tblActors(ActorID, ActorName) - to quickly find an actor or actress by name and his/her id
# tblDirectors(DirectorName, DirectorID) - to quickly find a director by name and his/her id
# (not used) tblBridge_Movie2Actor(MovieID) - to speed up bridge join
# (not used) tblBridge_Movie2Actor(ActorID) - to speed up bridge join
# (not used) tblBridge_Movie2Director(MovieID) - to speed up bridge join
# (not used) tblBridge_Movie2Director(DirectorID) - to speed up bridge join

import sqlite3

dbname = open("db_name.txt", "r").read()  # get the name of the db

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

c.execute("CREATE TABLE tblKeywords "
          "("
          "ID TEXT PRIMARY KEY, "
          "Keyword TEXT, "  # put an index on this column
          "MovieID TEXT, "  # contains a movieID. This means that a movie will have several rows in this table
          "tfidfVal REAL, "  # contains the tfidf value for the keyword in the movie
          "FOREIGN KEY(MovieID) REFERENCES tblMovies(MovieID)"
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

# bridge to handle many:many relationships
c.execute("CREATE TABLE tblBridge_Movie2Actor "
          "("
          "ID TEXT PRIMARY KEY, "
          "MovieID TEXT, "
          "ActorID TEXT, "
          "FOREIGN KEY(MovieID) REFERENCES tblMovies(MovieID), "
          "FOREIGN KEY(ActorID) REFERENCES tblActors(ActorID)"
          ");")

# bridge to handle many:many relationships
c.execute("CREATE TABLE tblBridge_Movie2Director "
          "("
          "ID TEXT PRIMARY KEY, "
          "MovieID TEXT, "
          "DirectorID TEXT, "
          "FOREIGN KEY(MovieID) REFERENCES tblMovies(MovieID), "
          "FOREIGN KEY(DirectorID) REFERENCES tblDirectors(DirectorID)"
          ");")

# storing information from python objects in db:
# https://stackoverflow.com/questions/2047814/is-it-possible-to-store-python-class-objects-in-sqlite

# create needed indices
c.execute("CREATE INDEX index_movies on tblMovies(MovieID, MovieName);")
c.execute("CREATE INDEX index_keywords on tblKeywords(Keyword);")
c.execute("CREATE INDEX index_actors on tblActors(ActorName, ActorID);")
c.execute("CREATE INDEX index_directors on tblDirectors(DirectorName, DirectorID);")
# c.execute("CREATE INDEX index_movie2actor_movie on tblBridge_Movie2Actor(MovieID);")
# c.execute("CREATE INDEX index_movie2actor_actor on tblBridge_Movie2Actor(ActorID);")
# c.execute("CREATE INDEX index_movie2director_movie on tblBridge_Movie2Director(MovieID);")
# c.execute("CREATE INDEX index_movie2director_director on tblBridge_Movie2Director(DirectorID);")

# apply changes
conn.commit()
print("Success! The movie database was created!")
conn.close()
