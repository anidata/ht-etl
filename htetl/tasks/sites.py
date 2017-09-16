'''Luigi tasks that manage various aspects of data on external sites'''

import htetl.extract.sites as sites


class FindExternalSites(luigi.Task):

    '''Extract external sites from web pages and dump to file'''

    def output(self):
        return luigi.LocalTarget('data/sites.txt')

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

        with self.output().open('w') as f:
            for site_url in site_list:
                f.write(site_url, '\n')
