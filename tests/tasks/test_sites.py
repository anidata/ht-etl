import luigi
import luigi.configuration

import htetl.tasks.sites as tasks_sites
import htetl.extract.sites as extract_sites
from .. import test_data, test_utils


def setup_module():
    test_config = luigi.configuration.LuigiConfigParser.instance()
    test_config.set('QueryPostgres', 'host', 'localhost')
    test_config.set('QueryPostgres', 'database', 'testdb')
    test_config.set('QueryPostgres', 'user', 'root')
    test_config.set('QueryPostgres', 'password', 'password')


class TestFindExternalUrls(object):

    '''Test FindExternalSites luigi task'''

    def setup(self):
        self.task = tasks_sites.FindExternalSites()

    def test_run(self):
        with test_utils.mock_targets(self.task) as task:
            inputs = task.input()
            with inputs['pages'].open('w') as f:
                f.write(test_data.RAWPAGEDATA_RAW__PAGE_CSV_CONTENTS)
            with inputs['base_sites'].open('w') as f:
                f.write(test_data.BASESITES_BASE_SITES__CSV_CONTENTS)

            task.run()
            assert task.complete()
