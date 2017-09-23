'''Luigi tasks that manage various aspects of data on external sites'''

import luigi
import pandas as pd

import htetl.extract.sites as sites
from htetl.tasks.loadpages import RawPageData
import htetl.util as util


class BaseSites(util.QueryPostgres):

    '''Return list of sites identifed as data scraping sources with IDs'''
    sql = 'select "Id", "Authority" from "BaseSites"'

    def output(self):
        in_path = 'data/base_sites.csv'
        return luigi.LocalTarget(in_path)


class FindExternalSites(luigi.Task):

    '''Extract external sites from web pages and dump to file'''

    def requires(self):
        return {
            'pages': RawPageData(),
            'base_sites': BaseSites(),
        }

    def output(self):
        return luigi.LocalTarget('data/sites.txt')

    def run(self):
        with self.input()['pages'].open('r') as f:
            pages = pd.read_csv(f)
        with self.input()['base_sites'].open('r') as f:
            base_sites = pd.read_csv(f, index_col='Authority')

        out = []
        for df_index, row in pages.iterrows():
            site_list = sites.extract_sites(row['Content'])
            out.extend([
                {
                'PageId': row.id,
                'ExternalSiteId': base_sites.iloc[external_site]['Id']
                }
                for external_site in site_list
            ])

        with self.output().open('w') as f:
            pd.DataFrame(out).to_csv(f)
