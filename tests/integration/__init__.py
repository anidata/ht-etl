import luigi
import os
import pandas as pd
import sqlalchemy
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from .. import test_data


POSTGRES_HOST = os.environ.get('TEST_POSTGRES_HOST', 'localhost')
POSTGRES_USER = os.environ.get('TEST_POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('TEST_POSTGRES_PASSWORD', '')
POSTGRES_DATABASE = os.environ.get('TEST_POSTGRES_DATABASE', 'test_db')


def sqlalchemy_engine():
    return sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}/{dbname}'.format(
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, dbname=POSTGRES_DATABASE
        )
    )

def setup_package():
    engine = sqlalchemy_engine()
    meta = sqlalchemy.MetaData()
    meta.reflect(bind=engine)
    meta.drop_all(engine)

    test_config = luigi.configuration.LuigiConfigParser.instance()
    test_config.set('QueryPostgres', 'host', POSTGRES_HOST)
    test_config.set('QueryPostgres', 'database', POSTGRES_DATABASE)
    test_config.set('QueryPostgres', 'user', POSTGRES_USER)
    test_config.set('QueryPostgres', 'password', POSTGRES_PASSWORD)

    pd.read_csv(
        StringIO(test_data.RAWPAGEDATA_RAW__PAGE_CSV_CONTENTS)
    ).to_sql(
        'page', engine,
        if_exists='replace',
        index=False,
    )

    pd.read_csv(
        StringIO(test_data.BASESITES_BASE_SITES__CSV_CONTENTS)
    ).to_sql(
        'BaseSites', engine,
        if_exists='replace',
        index=False,
    )

    pd.read_csv(
        StringIO(test_data.BACKPAGEEMAIL_CSV_CONTENTS)
    ).to_sql(
        'backpageemail', engine,
        if_exists='replace',
        index=False,
    )

    pd.read_csv(
        StringIO(test_data.BACKPAGEPHONE_CSV_CONTENTS)
    ).to_sql(
        'backpagephone', engine,
        if_exists='replace',
        index=False,
    )

    pd.read_csv(
        StringIO(test_data.BACKPAGEPOST_CSV_CONTENTS)
    ).to_sql(
        'backpagepost', engine,
        if_exists='replace',
        index=False,
    )
