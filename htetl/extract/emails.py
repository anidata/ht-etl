import re
import pandas as pd

# Variables used to compose the regular expression for
# extracting emails from text. NB: We need to also find
# emails that were attempted to be hidden by using "at"
# and "dot" instead of "@" or "."
NAME_SEPARATOR = "(@| at )"
NAME = "([\w._-]+)"
DOMAIN = "([\w._-]+)"
TLD_SEPARATOR = "(\.| dot )"
TLD = "(\w+)"
EMAIL_RE = re.compile(
    "".join([
        NAME, NAME_SEPARATOR, DOMAIN,
        TLD_SEPARATOR, TLD
    ])
)

def extract_emails(df):
    ''' Extracts emails from the page table csv

    :df: (pd.DataFrame) DataFrame containing content to be parsed
    :returns: Dataframe containing pageids and emails

    '''

    # For each record in the page table, find all email addresses and
    # create an array of tuples containing the pageid and the corresponding
    # email address. NB: each page may contain more than on email address.
    return pd.DataFrame(
        [
            (pageid,"".join([email[0], "@", email[2], ".", email[4]]))
            for pageid,body in df[["id", "content"]].values
            for email in EMAIL_RE.findall(body)
        ],
        columns=["pageid", "email"]
    )
