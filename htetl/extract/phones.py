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
    outfile = 'data/page_emails.csv'

    def requires(self):
        return loadpages.RawPageData()

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def run(self):

        with open(self.output().path, 'a') as f:  
	    # write posting id & phones to CSV
            f.write("MEOW")
