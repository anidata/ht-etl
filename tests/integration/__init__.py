import luigi
import os
import pandas as pd
import sqlalchemy
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

from .. import test_data


POSTGRES_HOST = os.environ.get('TEST_POSTGRES_HOST', 'localhost')
POSTGRES_USER = os.environ.get('TEST_POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('TEST_POSTGRES_PASSWORD', '')
POSTGRES_DATABASE = os.environ.get('TEST_POSTGRES_DATABASE', 'test_db')


def setup_package():
    engine = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@{host}/{dbname}'.format(
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, dbname=POSTGRES_DATABASE
        )
    )

    test_config = luigi.configuration.LuigiConfigParser.instance()
    test_config.set('QueryPostgres', 'host', POSTGRES_HOST)
    test_config.set('QueryPostgres', 'database', POSTGRES_DATABASE)
    test_config.set('QueryPostgres', 'user', POSTGRES_USER)
    test_config.set('QueryPostgres', 'password', POSTGRES_PASSWORD)

    # meta = sqlalchemy.MetaData(engine)
    # meta.reflect()
    # for table_name in ('Page', 'BaseSites'):
    #     try:
    #         meta.tables[table_name].drop()
    #     except KeyError:
    #         continue

    pd.read_csv(
        StringIO(test_data.RAWPAGEDATA_RAW__PAGE_CSV_CONTENTS)
    ).to_sql('Page', engine)
    pd.read_csv(
        StringIO(test_data.BASESITES_BASE_SITES__CSV_CONTENTS)
    ).to_sql('BaseSites', engine)
#
#
# def teardown_package():
#     engine = sqlalchemy.create_engine(
#         'postgresql://{user}:{password}@{host}/{dbname}'.format(
#             user=POSTGRES_USER, password=POSTGRES_PASSWORD,
#             host=POSTGRES_HOST, dbname=POSTGRES_DATABASE
#         )
#     )
#     meta = sqlalchemy.MetaData(engine)
#     meta.reflect()
#     meta.drop_all()
