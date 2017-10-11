import luigi
import luigi.configuration

import htetl.tasks.main as main
from ... import test_data, test_utils


DATA_FLAT_PHONE_CSV_CONTENTS = (
'''backpagepostid,number
1,123-123-1234
2,321-321-4321
3,123-123-1234
''')


DATA_FLAT_EMAIL_CSV_CONTENTS = (
'''backpagepostid,name
2,someone@somewhere.com
4,someone@somewhere.com
''')


DATA_FLAT_OID_CSV_CONTENTS = (
'''backpagepostid,oid
1,1
2,2
3,3
4,4
5,12321
6,12321
''')


class TestMakeGraphOld(object):

    '''Test MakeGraph luigi task'''

    def setup(self):
        test_config = luigi.configuration.LuigiConfigParser.instance()
        test_config.set('htetl-flags', 'new_data_extractor', 'false')
        self.task = main.MakeGraph()

    def test_run(self):
        data = {
            'data/flat_phone.csv': DATA_FLAT_PHONE_CSV_CONTENTS,
            'data/flat_email.csv': DATA_FLAT_EMAIL_CSV_CONTENTS,
            'data/flat_oid.csv': DATA_FLAT_OID_CSV_CONTENTS,
        }

        with test_utils.mock_targets(self.task) as task:
            for target in luigi.task.flatten(task.input()):
                with target.open('w') as f:
                    f.write(data[target.path])

            task.run()
            assert task.complete()

            with task.output().open('r') as f:
                data = f.read().strip().split('\n')
                expected = [
                    'entity_id,backpagepostid',
                    '0,1',
                    '1,2',
                    '0,3',
                    '1,4',
                    '2,5',
                    '2,6',
                ]
                assert set(data) == set(expected), ('%s != %s' % (str(data), str(expected)))
