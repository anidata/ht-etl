import luigi
from  htetl import util

class RawPageData(util.QueryPostgres):
    """ load eaxmple """
    sql = 'select id, content from page'

    def output(self):
        in_path = 'data/raw_page.csv'
        return luigi.LocalTarget(in_path)

