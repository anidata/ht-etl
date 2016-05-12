import luigi
import pandas as pd


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

        df.to_csv(self.output().path, index=None)

