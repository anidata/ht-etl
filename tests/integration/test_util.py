'''Integration tests for htetl.util'''
from StringIO import StringIO

import luigi
import luigi.mock
import pandas as pd

import htetl.util as util
from .. import test_data


class QueryPostgresTestChild(util.QueryPostgres):

    '''Child class of QueryPostgres to facilitate testing'''

    sql = 'SELECT * FROM "page"'

    def output(self):
        return luigi.mock.MockTarget('test.csv')


class TestITQueryPostgres(object):

    '''Test QueryPostgres luigi task'''

    def setup(self):
        self.task = QueryPostgresTestChild()

    def test_run(self):
        '''Test QueryPostgres.run() basic functionality'''
        luigi.build([self.task], local_scheduler=True)

        with self.task.output().open('r') as f:
            data = f.read()
            assert (
                pd.read_csv(StringIO(data)).equals(
                    pd.read_csv(StringIO(
                        test_data.RAWPAGEDATA_RAW__PAGE_CSV_CONTENTS
                    ))
                )
            )
