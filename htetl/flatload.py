from . import util
from . import email


class LoadReverseEmail(util.LoadPostgres):
    table = "flat_email"

    columns = [("backpagepostid", "text"),
               ("name", "text")]

    header = True

    def requires(self):
        return email.RawEmailData()


