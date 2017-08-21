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


class RawHTMLPostData(util.QueryPostgres):
    '''
     Load raw HTML Backpage posts from Postgres database and save to a CSV file
     using parent class's run() method.
     The posts can be loaded & processed by subsequent Luigi Tasks that depend
     on this one to make the CSV.
    '''

    sql = 'SELECT id,body FROM backpagecontent LIMIT 10;'
    
    def output(self):
        in_path = 'data/flat_post.csv'
        return luigi.LocalTarget(in_path)