'''Handy utility functions for testing'''
from contextlib import contextmanager
import mock

import luigi.mock


@contextmanager
def mock_targets(task):
    '''Replaces both input and output targets for a Luigi task with
    luigi.mock.MockTarget

    This makes the targets in-memory targets instead of their original type
    to facilitate/speed up testing

    '''

    def _update(target):
        try:
            return luigi.mock.MockTarget(target.path)
        except AttributeError:
            try:
                return {t: _update(target[t]) for t in target}
            except KeyError:
                return [_update(t) for t in target]

    task._orig_input = task.input
    task._orig_output = task.output

    task.input = mock.MagicMock(
        spec=task.input,
        return_value=_update(task.input())
    )
    task.output = mock.MagicMock(
        spec=task.output,
        return_value=_update(task.output())
    )

    yield task

    task.input = task._orig_input
    task.output = task._orig_output
    del task._orig_input
    del task._orig_output
