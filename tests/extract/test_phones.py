from htetl.extract.phones  import extract_phone
import os
import pandas as pd
from pandas.util.testing import assert_frame_equal

def test_extract_phones():
    df = pd.DataFrame(
        [
            (1, "blah blah 123-456-7890"),
            (2, "No phone number here!"),
            (3, "000-000-0000 and another one twothree4-ONE45-zeRO5fIve5")
        ],
        columns=["id", "content"]
    )
    expected = pd.DataFrame(
        [
            (1, "123-456-7890"),
            (3, "000-000-0000"),
            (3, "234-145-0555"),
        ],
        columns=["pageid", "phone"]
    )
    phone_df = extract_phone(df)
    assert_frame_equal(phone_df,expected)
    #assert all(phone_df == expected)

def test_no_numbers():
    df = pd.DataFrame(
        [
            (1, "blah blah 123-456-789"),
            (2, "No phone number here!"),
            (3, "000-000dashs0000")
        ],
        columns=["id", "content"]
    )

    phone_df = extract_phone(df)
    assert phone_df.empty
