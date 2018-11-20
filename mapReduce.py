from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk


class TFIDF(MRJob):

    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nodes,
                   reducer=self.reducer_combine_friend_nodes
                   ),
            MRStep(
                mapper=self.mapper_deem_evenodd,
                reducer=self.reducer_sum_evenodd
            )
        ]
    """

    # mapper: (documentid, content) -> ((term, documentid), 1)
    def mapper_tf(self, _, line):
        movieID, moviePlot = line.partition("||")  # use || as a delimiter
        tokens = nltk.word_tokenize(moviePlot)

        yield movieID, tokens

    # reducer: groups by key, sums values
    # reducer: ((term, documentid), tf)
    def reducer_tf(self, key, values):
        #print(key, values)
        yield key, values


if __name__ == '__main__':
    TFIDF.run()
