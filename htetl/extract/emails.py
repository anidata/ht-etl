import re
import pandas as pd
import luigi
import logging
from htetl.tasks import loadpages
from htetl import util

# Variables used to compose the regular expression for
# extracting emails from text. NB: We need to also find
# emails that were attempted to be hidden by using "at"
# and "dot" instead of "@" or "."
NAME_SEPARATOR = "(@| at )"
NAME = "([\w._-]+)"
DOMAIN = "([\w._-]+)"
TLD_SEPARATOR = "(\.| dot )"
TLD = "(\w+)"
EMAIL_RE = re.compile(
    "".join([
        NAME, NAME_SEPARATOR, DOMAIN,
        TLD_SEPARATOR, TLD
    ])
)

def extract_emails(df):
    ''' Extracts emails from the page table csv

    :df: (pd.DataFrame) DataFrame containing content to be parsed
    :returns: Dataframe containing pageids and emails

    '''

    # For each record in the page table, find all email addresses and
    # create an array of tuples containing the pageid and the corresponding
    # email address. NB: each page may contain more than on email address.
    return pd.DataFrame(
        [
            (pageid,"".join([email[0], "@", email[2], ".", email[4]]))
            for pageid,body in
            df[["id", "content"]].values
            for email in EMAIL_RE.findall(body)
        ],
        columns=["pageid", "email"]
    )


class ParseEmails(luigi.Task):
    '''
        Parses emails from raw Backpage posts & saves emails / post IDs in CSV file
    '''
    outfile = 'data/parsed_email.csv'

    def requires(self):
        return loadpages.RawPageData()

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def run(self):
        in_path = self.input().path
        df = pd.read_csv(in_path)
        email_df = extract_emails(df)
        with open(self.output().path, 'a') as f:  # write posting id & emails to CSV
            email_df.to_csv(f, index=None, encoding='utf-8')


class EmailsToPostgres(util.LoadPostgres):
    '''
        Loads CSV file of parsed emails / posting ids and saves to Postgres table.
        NB: The way luigi.postgres.CopyToTable is set up, if you run this
        twice in a row it won't overwrite the existing table. To make it save
        a new table, in Postgres command line or pgAdmin you have to drop that table
        AND the table called "table_updates" (or at least the "Emails_to_Postgres" row).
        Otherwise Luigi will think the Task is already done, because it checks "table_updates".
    '''
    header = True
    table = 'emailaddress'
    columns = [("pageid", "INT"),
               ("email", "TEXT")]

    def requires(self):
        return ParseEmails(self.host, self.database, self.user, self.password)
