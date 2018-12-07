from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
import math

class TFIDF(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_tf,
                   reducer=self.reducer_tf
                   ),
            MRStep(mapper=self.mapper_n,
                   reducer=self.reducer_n
                   )
        ]


# ----- 1st round: calc the tf for each word in each moviePlot ----- #

    # process each line in the input file. Each line in file = 1 movie
    def mapper_tf(self, _, line):
        movieID, moviePlot, totalMovies = line.strip("\n").split("|||")
        tokens = nltk.word_tokenize(moviePlot)
        for token in tokens:
            token = token.lower()
            if token.isalpha():
                yield (token, movieID, totalMovies), 1


    # sums for each word in each movie, and for the movieCount key
    def reducer_tf(self, key, values):
        yield key, sum(values)  # example: (("train", "unstoppable"), 7) or ("movieCount", 6)


# ------ 2nd round: calc the number of documents that has each unique word ------ #

    # key = (term, movie), example = ("train", "unstoppable")
    def mapper_n(self, key, tf):
        term = str(key[0])
        movieID = str(key[1])
        totalMovies = key[2]
        yield term, (tf, movieID, totalMovies, 1)  # unique per movie

    # values = list of tuples = [(tf, movie, 1), (tf, movie, 1), (tf, movie, 1) ...]
    def reducer_n(self, term, values):  # other is a generator object
        listvalues = list(values)
        n = float(sum([i[3] for i in listvalues]))  # sum the 1's in each of the lists in the values list to get the number of documents containing the term

        for item in listvalues:  # for each term in each movie
            tf = float(item[0])
            movieID = str(item[1])
            totalMovies = float(item[2])
            tfidf = math.log(totalMovies / n) * tf  # movieCount = same for all terms, n = same for a term across all movies, tf = differs for each term in each movie
            if tfidf > 6.0:
                yield (term, movieID), tfidf



if __name__ == '__main__':
    TFIDF.run()
