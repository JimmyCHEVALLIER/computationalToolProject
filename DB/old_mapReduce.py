from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
import ast
import math
import os
import sys
import sqlite3

# todo: test multiple yields in the mappers with an identifier showing piece of info is being yielded
# todo: find a way to only yield the top x keywords for each movie

# purpose of this class: calc the tf-idf of each word in each movie plot and find the most important words

#termCount = 0
#processedCount = 0

class TFIDF(MRJob):



    def steps(self):
        return [
            MRStep(mapper=self.mapper_tf,
                   reducer=self.reducer_tf
                   ),
            MRStep(mapper=self.mapper_n,
                   reducer=self.reducer_n),
            MRStep(mapper=self.mapper_tfidf)
        ]


# ----- 1st round: calc the tf for each word in each moviePlot ----- #

# input for mapper: list of tuples containing movieID and moviePlot
# split the list by getting number of movies len(list) and yield in loop

    # mapper: (movieID, moviePlot) -> ((word, movieID), 1)
    # input format is the c.fetchall result format: [(movieId, plot),(movieId, plot)]
    def mapper_tf(self, _, list):
        #global termCount
        list = ast.literal_eval(list)  # used for testing to convert a string mirroring a list into an actual list
        #print(list[0])

        # stemmer testing - conclusion: do not use any stemmer, since they all produce some weird words
        #sno = nltk.stem.SnowballStemmer('english')  # create snowball stemmer
        #ps = nltk.stem.PorterStemmer()
        #ps2 = porter2

        numberMovies = int(len(list))  # get the total number of movies in the list - this is also the total number of documents in the corpus
        #print("number of movies =", numberMovies)  # debugging
        for i in range(numberMovies):
            # identify movieID and moviePlot
            movieID = list[i][0]  # get movieID of the i'th movie
            moviePlot = list[i][1]  # get moviePlot of the i'th movie
            tokens = nltk.word_tokenize(moviePlot)  # tokenize the plot string using nltk (NLP)
            for word in tokens:
                if word not in nltk.corpus.stopwords.words() and word.lower().isalpha():  # transform in lower case and drop numbers and stopwords
                    yield (word.lower(), movieID, numberMovies), 1

    # reducer sums values for each key (group by key)
    # reducer: ((word, movieID), 1) -> ((word, movieID), tf)
    # key = (word, movieID)
    def reducer_tf(self, key, values):
        #global termCount
        #termCount += 1
        #sys.stderr.write(str(termCount) + " ")
        yield key, sum(values)


# ------ 2nd round: calc the number of documents that has each unique word ------ #

# count the word-document pairs for each word

    # mapper: ((word, movieID), tf)  -> (word, (movieID, tf, 1))
    def mapper_n(self, key, tf):
        word = str(key[0])
        movie = str(key[1])
        numberMovies = key[2]
        yield word, (movie, numberMovies, tf, 1)  # unique per movie


    # reducer: (word, (movieID, tf, 1)) -> ((word, documentId), (tf, n))
    def reducer_n(self, word, other):  # other is a generator object

        # other = [[movie, tf, 1],[movie, tf, 1],[movie, tf, 1]]

        #print(type(other))
        #n = sum(1 for _ in other)
        #for i in other:
        #print(word, i)
        #print((word, i[0]), (i[1], i[2]))

        #print(word, list(other), len(list(other)))  # cannot return length, because can only use generator once

        #for counter, element in enumerate(list(other)):
        #    print(counter, element)

        #for i in other:
        #    yield (word, i[0]), (i[1], len(list(other)))

        #test = next(other)[1]
        #print(word, test)

        """
        # sum to get the number of docs containing the word
        nDocs = []
        for i in other:
            #print(word, "|| movieId =>", i[0], "|| tf=>", i[1], "|| n (to sum up) =>", i[2])
            movieID = i[0]
            tf = i[1]
            nDocs.append(i[2])

        yield word, sum(nDocs)
        """

        listother = list(other)
        n = len(listother)
        for i in listother:
            movieId = i[0]
            numberMovies = i[1]
            tf = i[2]
            # print((word, movieId), (tf, n))
            yield (word, movieId), (tf, n, numberMovies)

        """
        for item in other:
            field = 0
            for i in item:
                if field == 0:
                    movie = i
                    field += 1
                elif field == 1:
                    tf = i
                    field += 1
                else:
                    n = i
                    field += 1

        yield (word, movie), (tf, n)
        """

    def mapper_tfidf(self, id, numbers):
        #global processedCount
        #global termCount

        tfidf = math.log((float(numbers[2]) / float(numbers[1]))) * numbers[0]

        #processedCount += 1
        #sys.stderr.write(str(termCount) + "-" + str(processedCount))
        #if float(processedCount) / float(termCount) in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
            #print("-----> Processed", (processedCount / termCount) * 100, "%")
        #    sys.stderr.write("-----> Processed", (processedCount / termCount) * 100, "% \n")

        if tfidf > 10.0:  # only get words of some importance
            yield id, tfidf

        #toinput = str(id) + " " + str(tfidf)
        #print(toinput)

        #f = open("keywords.txt", "a")
        #f.write(toinput)
        #f.close()
        #pass

if __name__ == '__main__':
    TFIDF.run()

