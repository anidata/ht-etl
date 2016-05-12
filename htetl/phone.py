from . import util
import luigi


class RawPhoneData(util.QueryPostgres):

    sql = 'select * from backpagephone'

    def output(self):
        in_path = 'data/flat_phone.csv'
        return luigi.LocalTarget(in_path)


