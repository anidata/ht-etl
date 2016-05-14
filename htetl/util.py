""" batch processing using luigi """

import luigi
import luigi.postgres
import pandas as pd
import psycopg2
import logging
import abc
import string


logger = logging.getLogger('luigi-interface')

logger.setLevel(logging.DEBUG)


class QueryPostgres(luigi.Task):
    """
    Base class to pull data from postgres and write to csv.

    When inheriting, you must implement:
        sql
        output
    """
    host = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                            name='host'),
                           significant=False)
    database = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                                name='database'),
                               significant=False)
    user = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                            name='user'),
                           significant=False)
    password = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                                name='password'),
                               significant=False)
    chunksize = 10000

    def run(self):
        # create db connection
        self.conn = psycopg2.connect(
            dbname=self.database,
            user=self.user,
            host=self.host,
            password=self.password)

        df_iterator = pd.read_sql_query(self.sql,
                                        self.conn,
                                        chunksize=self.chunksize)

        with open(self.output().path, 'a') as f:
            for ind, df in enumerate(df_iterator):
                if ind == 0:
                    df.to_csv(f, index=None)
                else:
                    df.to_csv(f, index=None, header=None)

                lines_written = ind * self.chunksize + len(df)
                logger.info(str(lines_written) + " lines written")

        self.conn.close()


class LoadPostgres(luigi.postgres.CopyToTable):
    host = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                            name='host'),
                           significant=False)
    database = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                                name='database'),
                               significant=False)
    user = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                            name='user'),
                           significant=False)
    password = luigi.Parameter(config_path=dict(section='QueryPostgres',
                                                name='password'),
                               significant=False)
    null_values = ['']
    header = False
    seperator = ','

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            for k, line in enumerate(fobj):
                if k == 0 and self.header:
                    continue
                row_str = filter(lambda x: x in string.printable, line)
                row_str = row_str.strip('\n').split(self.seperator)
                yield row_str

    @abc.abstractproperty
    def table(self):
        return None
