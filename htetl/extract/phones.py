import luigi
import logging
import pandas as pd
import string
from htetl.tasks import loadpages
from htetl import util


class ParsePhones(luigi.Task):
    '''
        Parses phone numbers from raw page data & saves phone numbets / post IDs in CSV file
    '''
    #host = luigi.Parameter(significant=False)
    #database = luigi.Parameter(significant=False)
    #user = luigi.Parameter(significant=False)
    #password = luigi.Parameter(significant=False)
    outfile = 'data/page_emails.csv'

    def requires(self):
        return loadpages.RawPageData() #self.host, self.database, self.user, self.password)

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def run(self):
        in_path = self.input().path
        #logger.info("Processing {}".format(in_path))
        #df = pd.read_csv(in_path)
        # (...) = make a capture group - at least one is required for pandas Series.str.extract method
        # [\w._-] = match any alphanumeric character (\w) or . or _ or -
        # + = match preceding expression one or more times
        # @ = match @
        # \. = match .
        #df["email"] = df["body"].str.extract("([\w._-]+@[\w_-]+\.\w+)", expand=True)
        #df = df.drop('body', 1)
        #df = df.dropna(axis=0, how='any')
        #if not df["email"].str.extract('(,)', expand=True).dropna(axis=0, how='any').empty:
        #    raise ValueError(' '.join(['At least one parsed email address contains a comma,',
        #                     'which may cause problems when other code loads', self.outfile, 'with comma separator']))

        with open(self.output().path, 'a') as f:  # write posting id & emails to CSV
            f.write("MEOW")
	    #df.to_csv(f, index=None, encoding='utf-8')
