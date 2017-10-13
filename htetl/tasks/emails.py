import pandas as pd
import luigi
from htetl.tasks import loadpages
from htetl import util


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
