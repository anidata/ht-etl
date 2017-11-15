from htetl.extract.emails import extract_emails
import pandas as pd

def test_extract_emails():
    df = pd.DataFrame(
        [
            (1, "foo@bar.com bar at baz dot com"),
            (2, "these aren't the emails you're looking for"),
            (3, "other text spam at ham.eggs aroud the email")
        ],
        columns=["id", "content"]
    )
    expected = pd.DataFrame(
        [
            (1, "foo@bar.com"),
            (1, "bar@baz.com"),
            (3, "spam@ham.eggs"),
        ],
        columns=["pageid", "email"]
    )
    email_df = extract_emails(df)
    assert email_df.equals(expected)
