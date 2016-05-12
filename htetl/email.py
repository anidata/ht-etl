from . import util
import luigi


class RawEmailData(util.QueryPostgres):

    """ load eaxmple """
    sql = 'select * from backpageemail'

    def output(self):
        in_path = 'data/flat_email.csv'
        return luigi.LocalTarget(in_path)
