'''Integration tests for htetl.tasks.sites'''
import luigi

import htetl.tasks.sites as tasks_sites


class TestITBaseSites(object):

    '''Test BaseSites luigi task'''

    def setup(self):
        self.task = tasks_sites.BaseSites()

    def test_run(self):
        '''Test BaseSites.run() basic functionality'''
        luigi.build([self.task], local_scheduler=True)
        assert self.task.complete()
