import luigi
from  htetl import util

class RawPageData(util.QueryPostgres):
    """ load eaxmple """
    sql = 'select Id, Content from page limit 100'

    def output(self):
        in_path = 'data/raw_page.csv'
        return luigi.LocalTarget(in_path)

