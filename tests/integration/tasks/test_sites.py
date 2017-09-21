'''Integration tests for htetl.tasks.sites'''
import htetl.tasks.sites as tasks_sites

class TestITBaseSites(object):

    '''Test BaseSites luigi task'''

    def setup(self):
        self.task = tasks_sites.BaseSites()

    def test_run(self):
        '''Test BaseSites.run() basic functionality'''
        self.task.run()
        assert self.task.complete()
