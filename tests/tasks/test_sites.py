import htetl.tasks.sites as tasks_sites
import htetl.extract.sites as extract_sites

def test_FindExternalUrls():
    task = tasks_sites.FindExternalSites()
    task.run()
    assert task.complete()
