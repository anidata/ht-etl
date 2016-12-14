""" Example of processing data from file on disk """

import luigi
import pandas as pd
from . import util


class HtTasks(luigi.WrapperTask):
    """ Runs all tasks """

    def requires(self):
        yield LoadReverseUrl()


class RawFlatData(luigi.ExternalTask):
    """ specify that an external task has created the input data """

    def output(self):
        in_path = 'data/flat_data.csv'
        return luigi.LocalTarget(in_path)


class ReverseUrl(luigi.Task):
    """  reversing url task """

    def output(self):
        out_path = 'data/reverse_url.csv'
        return luigi.LocalTarget(out_path)

    def requires(self):
        return RawFlatData()

    def run(self):
        """ data is small enough to use pandas for the processing """
        df = pd.read_csv(self.input().path)

        df['reversed_url'] = df.url.map(lambda x: x[::-1])
        df = df[['url', 'reversed_url']]

        df.to_csv(self.output().path, index=None)


class LoadReverseUrl(util.LoadPostgres):
    table = "reverse_url"

    columns = [("url", "text"),
               ("reversed_url", "text")]

    header = True

    def requires(self):
        return ReverseUrl()
