import luigi
import logging
import pandas as pd
from htetl.tasks import loadpages
from htetl import util
from htetl.extract import phones as LoadPhones

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.DEBUG)



class ParsePhones(luigi.Task):
    '''
        Parses phone numbers from raw page data & saves phone numbets / post IDs in CSV file
    '''
    outfile = 'data/page_phones.csv'

    def requires(self):
        return loadpages.RawPageData()

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def run(self):
	in_path = self.input().path
        logger.info("Processing {}".format(in_path))
        df = pd.read_csv(in_path)
        # go over each html and grap phones
        phonedf = LoadPhones.extract_phone(df)
        with open(self.output().path, 'a') as f:
        # write posting id & phones to CSV
            phonedf.to_csv(f,index=False)

class PhonesToPostgres(util.LoadPostgres):
    '''
        Loads CSV file of parsed phones / posting ids and saves to Postgres table.
        NB: The way luigi.postgres.CopyToTable is set up, if you run this
        twice in a row it won't overwrite the existing table. To make it save
        a new table, in Postgres command line or pgAdmin you have to drop that table
        AND the table called "table_updates" (or at least the "Phones_to_Postgres" row).
        Otherwise Luigi will think the Task is already done, because it checks "table_updates".
    '''
    header = True
    table = 'phones'
    columns = [("pageid", "INT"),
               ("phone", "TEXT")]

    def requires(self):
        return ParsePhones()
