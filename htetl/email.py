from . import util
import luigi


class RawEmailData(util.QueryPostgres):

    # read parameters from luigi.cfg
    sql = 'select * from backpageemail'

    def output(self):
        in_path = 'data/flat_email.csv'
        return luigi.LocalTarget(in_path)


