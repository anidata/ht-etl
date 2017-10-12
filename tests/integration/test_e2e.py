'''End-to-end/smoke tests for htetl'''
import luigi
import luigi.mock

import htetl.tasks.main as tasks_main
from .. import test_data
from .. import integration


class TestITLoadEntityIds(object):

    '''Smoketest whole ETL pipeline'''

    def setup(self):
        self.config = luigi.configuration.LuigiConfigParser.instance()
        self.task = tasks_main.LoadEntityIds()

    def tearDown(self):
        engine = integration.sqlalchemy_engine()
        engine.execute('DROP TABLE IF EXISTS "BackpageEntities"')
        engine.execute('DROP TABLE IF EXISTS backpageentities')

    def test_run_old_ETL_flow(self):
        '''Smoketest LoadEntityIds with old ETL workflow'''
        self.config.set('htetl-flags', 'new_data_extractor', 'false')
        luigi.build([self.task], local_scheduler=True)

        assert self.task.complete()

    def test_run_new_ETL_flow(self):
        '''Smoketest LoadEntityIds with new ETL workflow'''
        self.config.set('htetl-flags', 'new_data_extractor', 'true')
        luigi.build([self.task], local_scheduler=True)

        assert self.task.complete()
