from . import util
import luigi


class RawPhoneData(util.QueryPostgres):

    # read parameters from luigi.cfg
    sql = 'select * from backpagephone'

    def output(self):
        in_path = 'data/flat_phone.csv'
        return luigi.LocalTarget(in_path)


