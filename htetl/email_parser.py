import re
import pandas as pd
import psycopg2

def get_posting_html(sql_query):
    """ Get raw html from PostgreSQL database (backpage postings).
        Database server must be running.
        sql_query = PostgreSQL query.
        Note: sql_query is assumed to be a SELECT statement
    """
    conn = psycopg2.connect( # create db connection
            dbname='crawler',
            user='postgres',
            host='localhost',
            password='Jalafel48')
    chunksize = 10000
    df_iterator = pd.read_sql_query(sql_query,
                                    conn,
                                    chunksize=chunksize)
    df = df_iterator.next()
    conn.close()
    return df

df = get_posting_html('SELECT id,body FROM backpagecontent;')
s = pd.Series(df.body)

#emails = s.str.extract("(?P<email>[\w._-]+[\s]+at[\s]+[\w_-]+[\s]+dot[\s]+com)", expand=True) # Note: extractall may be better because extract only gets 1st match
emails = s.str.extract("(?P<email>[\w._-]+\@[\w_-]+\.\w+)", expand=True) # Note: extractall may be better because extract only gets 1st match

# ?P<email> = name the column "email"
print(emails.dropna())

'''
emails = re.findall("[\w._-]+\@[\w_-]+\.\w+", message.source)
emails.extend(re.findall("[\w._-]+\@[\w_-]+\.\w+", message.url))


"[\w._-]+\@[\w_-]+\.\w+"

# https://docs.python.org/2/howto/regex.html#regex-howto

\. = match .
[\w._-] = match any alphanumeric character (\w) or . or _ or -
+ = match one or more times

SELECT body FROM crawler.public.backpagecontent LIMIT 100;
SELECT content FROM crawler.public.page LIMIT 100;

Examples of email address formats to handle (not exhaustive):

    some.one@somewhere.com
    some.one at somewhere.com
    some dot one @ some where .com
'''