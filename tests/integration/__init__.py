import pandas as pd
import psycopg2

from .. import test_data


# Should eventually allow environmental variable to configure this
POSTGRES_HOST = 'localhost'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = ''
POSTGRES_DATABASE = 'test_db'


def setup_package():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DATABASE
    )

    pd.DataFrame(test_data.RAWPAGEDATA_RAW__PAGE_CSV_CONTENTS).tosql(conn)
    pd.DataFrame(test_data.BASESITES_BASE_SITES__CSV_CONTENTS).tosql(conn)
