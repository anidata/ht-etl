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
        return {
            'page_sites': luigi.LocalTarget('data/sites.txt'),
            'site_updates': luigi.LocalTarget('data/site_updates.txt')
        }

    def run(self):
        with self.input()['pages'].open('r') as f:
            pages = pd.read_csv(f)
        with self.input()['base_sites'].open('r') as f:
            base_sites = pd.read_csv(f)

        site_dict = {
            r['Authority']: r['Id']
            for _, r in base_sites.iterrows()
        }
        new_site_id = max([int(v) for v in site_dict.values()]) + 1

        out = []
        site_updates = []
        for df_index, row in pages.iterrows():
            site_list = sites.extract_sites(row['content'])

            updates = []
            for external_site in site_list:
                try:
                    external_site_id = site_dict[external_site]
                except KeyError:
                    external_site_id = new_site_id
                    site_dict[external_site] = external_site_id

                    site_updates.append((new_site_id, external_site))
                    new_site_id += 1
                updates.append((row.id, external_site_id))

            out.extend(updates)

        # Remove duplicate entries
        out = list(set(out))
        with self.output()['page_sites'].open('w') as f:
            page_site_df = pd.DataFrame(
                out, columns=['PageId', 'ExternalSiteId']
            ).sort_values('PageId')
            page_site_df.to_csv(
                f,
                index=False,
                columns=['PageId', 'ExternalSiteId']
            )

        with self.output()['site_updates'].open('w') as f:
            pd.DataFrame(
                site_updates,
                columns=['Id', 'Authority']
            ).to_csv(
                f,
                index=False,
                columns=['Id', 'Authority']
            )
