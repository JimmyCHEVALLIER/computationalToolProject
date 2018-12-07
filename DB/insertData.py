import sqlite3
import os.path
import sys
import uuid
import json
import string


def generateWikiList():
    # generate list of letter accordingly to twitter category A,B,U-W,numbers...
    lists = list(string.ascii_lowercase[:9].upper())
    lists.append(['J-K', 'N-O', 'Q-R', 'U-W', 'X-Z', 'numbers'])
    lists.append(list(string.ascii_lowercase[11:13].upper()))
    lists.append(list(string.ascii_lowercase[15].upper()))
    lists.append(list(string.ascii_lowercase[18:20].upper()))
    lists = [item for sublist in lists for item in sublist]
    lists = sorted(lists)
    return lists


dbname = open("db_name.txt", "r").read()  # get the name of the db

# check if db exists
if not os.path.isfile(dbname):
    # raise ValueError("Error! The database does not exist")
    sys.exit("Error! The database does not exist")

conn = sqlite3.connect(dbname)  # creates the db if it does not already exist
c = conn.cursor()

# insert test data
c.execute(
    "INSERT INTO tblMovies values ('id1', 'The Great Movie', NULL, 'This is a movie plot this movie is about a happy family who lives in the city', 10, 20);")
conn.commit()
parseCount = 0
value = ""
for category in generateWikiList():  # [:9]:
    f = open('../_' + category + '.txt')
    res = json.load(f)
    for key in list(res.keys()):
        c.execute("SELECT MovieName FROM tblMovies WHERE MovieName = '" + key.replace("'", "\"") + "'")
        checkMovie = c.fetchall()
        if checkMovie == []:
            if "Release date" in list(res[key].keys()) and "Directed by" in list(
                    res[key].keys()) and "Starring" in list(res[key].keys()) and "Plot" in list(res[key].keys()) and \
                            res[key]['Plot'] != None and res[key]['Plot'] != "":
                movieId = str(uuid.uuid4())
                if "Box office" in list(res[key].keys()) and "Budget" in list(res[key].keys()):
                    tmpValue = "('", movieId, "',\'", key.replace("'", "\""), "\','", res[key]["Release date"][
                        0], "', \'", res[key]["Plot"].replace("'", ""), "\' , '", res[key]["Box office"][0], "' , '", \
                               res[key]["Budget"][0], "')"  # res[key]["Plot"].replace('"',"")
                    value = ''.join(tmpValue)
                else:
                    tmpValue = "('", movieId, "',\'", key.replace("'", "\""), "\','", res[key]["Release date"][
                        0], "', \'", res[key]["Plot"].replace("'",
                                                              ""), "\' , '", 'Null', "' , '", 'Null', "')"  # res[key]["Plot"].replace('"',"")
                    value = ''.join(tmpValue)

                if "Starring" in list(res[key].keys()):
                    for star in res[key]['Starring']:
                        tmpId = str(uuid.uuid4())
                        c.execute("SELECT ActorID FROM tblActors WHERE ActorName = '" + star.replace("'", " ") + "'")
                        resActor = c.fetchall()
                        if resActor == []:
                            c.execute(
                                "INSERT INTO tblActors VALUES ('" + tmpId + "','" + star.replace("'", " ") + "');")
                            c.execute("INSERT INTO tblBridge_Movie2Actor VALUES ('" + str(
                                uuid.uuid4()) + "','" + movieId + "','" + tmpId + "');")
                        else:
                            c.execute("INSERT INTO tblBridge_Movie2Actor VALUES ('" + str(
                                uuid.uuid4()) + "','" + movieId + "','" + resActor[0][0] + "');")

                if "Directed by" in list(res[key].keys()):
                    for director in res[key]['Directed by']:
                        tmpId = str(uuid.uuid4())
                        c.execute(
                            "SELECT DirectorID FROM tblDirectors WHERE DirectorName = '" + director.replace("'", " ") + "'")
                        resDirector = c.fetchall()
                        if resDirector == []:
                            c.execute("INSERT INTO tblDirectors VALUES ('" + tmpId + "','" + director.replace("'",
                                                                                                              " ") + "');")
                            c.execute("INSERT INTO tblBridge_Movie2Director VALUES ('" + str(
                                uuid.uuid4()) + "','" + movieId + "','" + tmpId + "');")
                        else:
                            c.execute("INSERT INTO tblBridge_Movie2Director VALUES ('" + str(
                                uuid.uuid4()) + "','" + movieId + "','" + resDirector[0][0] + "');")
                parseCount += 1
                c.execute("INSERT INTO tblMovies VALUES " + value + ";")
    conn.commit()

    print("Category => ", category, " done")

print("Total parsed numbers =>",
      parseCount)  # release date 1285 directed by 1282 starring 1206 Box office 559 Budget 408
print("done")

# select values
c.execute("SELECT count(MovieName) FROM tblMovies;")
print(c.fetchall()[0][0])

conn.close()


