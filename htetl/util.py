""" batch processing using luigi """

import luigi
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine


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
                df.to_csv(f, index=None)
                lines_writte = ind * self.chunksize + len(df)
                logger.info(str(lines_writte) + " lines written")

        self.conn.close()


class LoadPostgres(QueryPostgres):
    """
    Base class to pull data from postgres and write to csv with inferred
    columns.

    When inheriting, you must implement:
        input

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

        engine = create_engine(
            'postgresql://{user}:{password}@{host}/{dbname}'.format(
                dbname=self.database,
                user=self.user,
                host=self.host,
                password=self.password))

        c = engine.connect()
        conn = c.connection

        df_iterator = pd.read_csv(self.input().path,
                                  chunksize=self.chunksize)

        for ind, df in enumerate(df_iterator):
            lines_writte = ind * self.chunksize + len(df)
            df.to_sql('flat_no_body', engine)
            logger.info(str(lines_writte) + " lines loaded")


        self.conn.close()
