from . import util
import luigi


class RawEmailData(util.QueryPostgres):

    """ load eaxmple """
    sql = 'select backpagepostid,name from backpageemail'

    def output(self):
        in_path = 'data/flat_email.csv'
        return luigi.LocalTarget(in_path)


class RawPhoneData(util.QueryPostgres):

    sql = 'select backpagepostid,number from backpagephone'

    def output(self):
        in_path = 'data/flat_phone.csv'
        return luigi.LocalTarget(in_path)


class RawPosterData(util.QueryPostgres):

    sql = 'select id,oid from backpagepost'

    def output(self):
        in_path = 'data/flat_oid.csv'
        return luigi.LocalTarget(in_path)
