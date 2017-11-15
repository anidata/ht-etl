import random
import string
import time

from htetl.extract.phones import extract_phone
import pandas as pd

def test_extract_phone():
    df = pd.DataFrame(
        [
            (1, "123-123-1234 eight SEven six 876 NINE 8 7 6"),
            (2, "these aren't the phones you're looking for"),
            (3, "other text oNe 2 Three 1 two 3 four ThRee 2 1 aroud the phone")
        ],
        columns=["id", "content"]
    )
    expected = pd.DataFrame(
        [
            (1, "1231231234"),
            (1, "8768769876"),
            (3, "1231234321"),
        ],
        columns=["pageid", "phone"]
    )
    phone_df = extract_phone(df)

    assert phone_df.equals(expected)


def test_extract_phone_performance():
    post_count = 2500
    post_character_count = 25000
    worst_case_phone = 'one two three one two three one two three four'

    choice_string = string.ascii_uppercase + string.ascii_lowercase + string.digits
    data = [
        (
            i,
            worst_case_phone + ' ' + ''.join(
                random.choice(choice_string)
                for _ in range(post_character_count)
            )
        )
        for i in range(post_count)
    ]

    df = pd.DataFrame(data, columns=["id", "content"])

    start = time.time()
    extract_phone(df)
    end = time.time()
    duration = end - start

    # Threshold determined with assumption of 250k posts needing to be
    # analyzed in <6 hours (goal is the 250k posts should be completely
    # analyzed in <24 hours), with average of 25k characters in each post
    # (determined in data extract on 2017-11-13).
    assert post_count * 1.0 / duration >= (250000.0 / (6 * 60 * 60))


test_extract_phone_performance.slow = 1
