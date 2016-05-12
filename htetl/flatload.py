import pandas as pd
import luigi
from . import util


class RawFlatData(luigi.ExternalTask):
    """ specify that an external task has created the input data """

    def output(self):
        in_path = 'data/flat_data.csv'
        return luigi.LocalTarget(in_path)


class LoadFlatData(util.LoadPostgres):
    def requires(self):
        return RawFlatData()


