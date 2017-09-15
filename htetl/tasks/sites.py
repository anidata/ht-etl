'''Luigi tasks that manage various aspects of data on external sites'''

import htetl.extract.sites as sites


class FindExternalSites(luigi.Task):

    '''Extract external sites from web pages and dump to file'''

    def run(self):
        # Partially stolen from luigi.contrib.postgres.PostgresQuery
        # TODO: check if context manager can be used
        connection = self.input().connect()
        cursor = connection.cursor()

        cursor.execute(self.query)
        unique_sites = set([])
        for record in cursor:
            html = record[2]
            site_list = sites.extract_sites(html)
            unique_sites.update(*site_list)
