from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk


class TFIDF(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_tf,
                   reducer=self.reducer_tf
                   )
        ]


    # mapper: (documentid, content) -> ((term, documentid), 1)
    def mapper_tf(self, _, line):
        movieID, moviePlot = line.partition("||")  # use || as a delimiter
        tokens = nltk.word_tokenize(moviePlot)

        yield (movieID, tokens), 1

    # reducer: groups by key, sums values
    # reducer: ((term, documentid), tf)
    def reducer_tf(self, key, values):
        #print(key, values)
        yield key, sum(values)


if __name__ == '__main__':
    TFIDF.run()
