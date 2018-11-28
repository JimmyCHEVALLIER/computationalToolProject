from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk

# purpose of this class: calc the tf-idf of each word in the parsed movie plot and find the most important words
# this class is called on each of the movie plots separately as it is used in a loop of all the movies


class TFIDF(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_tf,
                   reducer=self.reducer_tf
                   ),
            MRStep(mapper=self.mapper_n,
                   reducer=self.reducer_n)
        ]

    # ----- 1st round: calc the tf for each word in each moviePlot ----- #

    # mapper: (moviePlot) -> (word, 1)
    # only the movie plot is passed as input
    def mapper_tf(self, _, line):
        # tokenize the plot string using nltk (NLP)
        tokens = nltk.word_tokenize(line)  # tokens is a list of the terms in the moviePlot string

        for word in tokens:
            yield word, 1

    # reducer sums values for each key (group by key)
    # reducer: (word, 1) -> (word, tf)
    def reducer_tf(self, word, values):
        yield word, sum(values)

    # ------ 2nd round: calc the number of documents that has each unique word ------ #

    # mapper: (word, tf)  -> (word,(tf,1))
    def mapper_n(self, word, tf):
        yield word, (tf, 1)

    # reducer: ((word, documentId), (tf, n))
    def reducer_n(self, word, doc_tf_n):
        print(word, doc_tf_n)


if __name__ == '__main__':
    TFIDF.run()

