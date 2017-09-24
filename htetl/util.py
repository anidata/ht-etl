""" batch processing using luigi """

import luigi
import luigi.contrib.postgres
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

        # write to output file
        with self.output().open('w') as f:
            for ind, df in enumerate(df_iterator):
                # Erase non-standard characters from string fields, like the Unicode character for a checkmark.
                string_filter = lambda cell: filter(lambda x: x in string.printable, cell) if type(cell)==str else cell
                df = df.applymap(string_filter) # prevents UnicodeDecodeError when trying to write unusual characters to CSV
                if ind == 0:
                    df.to_csv(f, index=None, encoding='utf-8')
                else:
                    df.to_csv(f, index=None, header=None, encoding='utf-8')
                lines_written = ind * self.chunksize + len(df)
                logger.info(str(lines_written) + " lines written")
        self.conn.close()


class LoadPostgres(luigi.contrib.postgres.CopyToTable):
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
                row_str = row_str.strip('\n\r').split(self.seperator)
                yield row_str

    @abc.abstractproperty
    def table(self):
        return None
