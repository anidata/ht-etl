import luigi
from . import email
from . import phone
from . import reverse_url


class HtTasks(luigi.WrapperTask):
    """ Runs all tasks """

    def requires(self):
        yield email.RawEmailData()
        yield phone.RawPhoneData()
        yield reverse_url.LoadReverseUrl()

