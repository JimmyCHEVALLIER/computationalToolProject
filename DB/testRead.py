import sqlite3

# if the database exists, then continue
conn = sqlite3.connect("movieFinderTestDb.sqlite")
c = conn.cursor()

# get all movies from the db that not not have keywords in the db
c.execute("SELECT movieid, movieplot FROM tblMovies WHERE movieid NOT IN (SELECT DISTINCT movieid FROM tblKeywords);")
conn.commit()
results = c.fetchall()  # format of items in results list: (movieid, movieplot) | each list item = a movie
countMovies = len(results)
print("number of movies =", countMovies)

open('mrJobInputTest.txt', 'w').close()  # clear the file
with open("mrJobInputTest.txt", "a") as f:
    for item in results:
        f.write(item[0] + "\t" + item[1] + "\t" + str(countMovies) + "\n")

with open("mrJobInputTest.txt", "r") as f:
    for line in f:
        movieid, movieplot, noMovies = line.strip("\n").split("\t")  # split each line into movieid and movieplot
        print(movieid, movieplot, noMovies)
